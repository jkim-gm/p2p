using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace P2PTransferLib
{
    /// <summary>
    /// P2P 송신자(서버) 모듈
    /// - 프로그램 내에 글로벌 변수 하나(또는 P2PSenderNode.Instance 싱글톤)만 두고 사용합니다.
    /// - chID(채널ID) + msgID(파일ID) 조합으로 파일을 등록하고, 수신자가 (peerID, peerName, chID, msgID)로 요청하면 백그라운드로 동시 전송합니다.
    /// - 잘못된 파일 경로, 포트 번호, 미등록 파일 요청, 네트워크 끊김 등 모든 예외 상황을 내부에서 안전하게 처리하고 OnError / OnLog로 상세 메시지를 전달합니다.
    /// </summary>
    public class P2PSenderNode : IDisposable
    {
        /// <summary>
        /// 어디서든 하나로 접근 가능한 글로벌 싱글톤 인스턴스
        /// </summary>
        public static P2PSenderNode Instance { get; } = new P2PSenderNode();

        public int Port { get; private set; }
        public bool IsRunning => _isRunning;

        /// <summary>
        /// 고정 패킷(조각) 크기: 기본 64KB
        /// </summary>
        public int ChunkSize { get; set; } = 64 * 1024;

        /// <summary>
        /// 등록된 파일의 유효 시간 (기본 30분)
        /// </summary>
        public TimeSpan ValidDuration { get; set; } = TimeSpan.FromMinutes(30);

        private int _maxConcurrentReceivers = 3;
        /// <summary>
        /// 최대 동시 전송 수 (기본 3). 초과하는 요청은 자동으로 대기(WaitQueue) 후 순차 전송됨.
        /// </summary>
        public int MaxConcurrentReceivers
        {
            get => _maxConcurrentReceivers;
            set
            {
                _maxConcurrentReceivers = Math.Max(1, value);
                ProcessWaitingQueue();
            }
        }

        private int _speedLimitKbps = 0;
        /// <summary>
        /// 전송 속도 제한 (KB/s). 0이면 무제한.
        /// </summary>
        public int SpeedLimitKbps
        {
            get => _speedLimitKbps;
            set => _speedLimitKbps = Math.Max(0, value);
        }

        /// <summary>
        /// 전송 중(Transferring)일 때 OnProgress 이벤트 및 로그를 발생시키는 최소 시간 간격 (밀리초 단위, 기본값: 500ms = 0.5초)
        /// </summary>
        public double ProgressIntervalMs { get; set; } = 500.0;

        private TcpListener _listener;
        private CancellationTokenSource _cts;
        private bool _isRunning;

        // 등록된 송신 파일 목록 (Key: "{chId}_{msgId}")
        private readonly ConcurrentDictionary<string, RegisteredFileItem> _registeredFiles = new ConcurrentDictionary<string, RegisteredFileItem>();

        // 수신자별 송신 상태 기록 목록 (Key: "{chId}_{msgId}_{peerId}")
        private readonly ConcurrentDictionary<string, SenderTransferStatusItem> _transferStatusMap = new ConcurrentDictionary<string, SenderTransferStatusItem>();

        // 현재 연결되어 전송 중인 활성 소켓 세션 목록 (Key: SessionKey)
        private readonly ConcurrentDictionary<string, ClientSession> _activeSessions = new ConcurrentDictionary<string, ClientSession>();

        // 동시 전송 제한 슬롯 및 대기열 관리
        private readonly object _queueLock = new object();
        private int _activeTransferCount = 0;
        private readonly Queue<TaskCompletionSource<bool>> _waitingQueue = new Queue<TaskCompletionSource<bool>>();

        /// <summary>
        /// 단순 로그 출력 이벤트
        /// </summary>
        public event Action<string> OnLog;

        /// <summary>
        /// 진행 상태 이벤트 (WaitingQueue, Transferring, Paused, Completed, Cancelled, Expired, Error, FileMissing 모두 전달됨)
        /// </summary>
        public event EventHandler<TransferProgressEventArgs> OnProgress;

        /// <summary>
        /// 상세 오류 이벤트 (잘못된 파일 등록, 포트 충돌, 미등록 파일 요청, 네트워크 오류 등 발생 시 호출됨)
        /// </summary>
        public event EventHandler<P2PErrorEventArgs> OnError;

        private class ClientSession
        {
            public string SessionKey { get; set; }
            public string PeerId { get; set; }
            public string PeerName { get; set; }
            public string ChId { get; set; }
            public string MsgId { get; set; }
            public RegisteredFileItem FileItem { get; set; }
            public int LastChunkIndex { get; set; }
            public long LastProgressTick { get; set; }
            public TcpClient Client { get; set; }
            public NetworkStream Stream { get; set; }
            public SemaphoreSlim WriteLock { get; } = new SemaphoreSlim(1, 1);
            public CancellationTokenSource Cts { get; set; }
            public bool IsPaused { get; set; }
            public bool IsCancelled { get; set; }
        }

        private async Task SendSessionFrameAsync(ClientSession session, P2PCommand cmd, int chunkIndex, byte[] hash32, byte[] payload, CancellationToken ct)
        {
            await session.WriteLock.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await FrameStreamHelper.WriteFrameAsync(session.Stream, cmd, chunkIndex, hash32, payload, ct).ConfigureAwait(false);
            }
            finally
            {
                session.WriteLock.Release();
            }
        }

        private Task SendSessionControlAsync(ClientSession session, P2PCommand cmd, int chunkIndex, string textPayload, CancellationToken ct)
        {
            byte[] payload = string.IsNullOrEmpty(textPayload) ? null : System.Text.Encoding.UTF8.GetBytes(textPayload);
            return SendSessionFrameAsync(session, cmd, chunkIndex, null, payload, ct);
        }

        public static string BuildFileKey(string chId, string msgId)
        {
            return $"{(chId ?? "").Trim()}_{(msgId ?? "").Trim()}";
        }

        /// <summary>
        /// 1. 송신자(서버) P2P TCP 포트를 백그라운드로 오픈합니다.
        /// 잘못된 포트 번호나 IP, 이미 사용 중인 포트일 경우 예외로 죽지 않고 오류 로그/이벤트를 발생시킵니다.
        /// </summary>
        public bool StartServer(int port, string bindIp = "127.0.0.1")
        {
            if (_isRunning)
            {
                Log($"[서버 안내] 이미 포트 {Port}에서 P2P 송신 서버가 실행 중입니다.");
                return true;
            }

            if (port <= 0 || port > 65535)
            {
                RaiseError(P2PErrorCode.InvalidParameter, null, null, null, null,
                    $"잘못된 포트 번호({port})입니다. 포트 번호는 1 ~ 65535 사이여야 합니다.");
                return false;
            }

            IPAddress ip = IPAddress.Any;
            if (!string.IsNullOrWhiteSpace(bindIp))
            {
                string trimmedIp = bindIp.Trim();
                if (trimmedIp == "127.0.0.1" || trimmedIp.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                {
                    ip = IPAddress.Loopback; // 로컬 테스트 시 방화벽 팝업 방지
                }
                else if (trimmedIp == "0.0.0.0" || trimmedIp == "*")
                {
                    ip = IPAddress.Any;
                }
                else if (!IPAddress.TryParse(trimmedIp, out ip))
                {
                    RaiseError(P2PErrorCode.InvalidParameter, null, null, null, null,
                        $"잘못된 바인딩 IP 주소 형식('{bindIp}')입니다. 올바른 IPv4 주소를 입력해주세요.");
                    return false;
                }
            }

            try
            {
                Port = port;
                _cts = new CancellationTokenSource();
                _listener = new TcpListener(ip, port);
                _listener.Start();
                _isRunning = true;

                Log($"[서버 시작] P2P 송신 서버 포트 오픈 완료 (Bind={ip}:{port}, 패킷크기={ChunkSize / 1024}KB, 동시제한={MaxConcurrentReceivers})");
                Task.Run(() => AcceptClientsLoopAsync(_cts.Token));
                return true;
            }
            catch (SocketException sockEx)
            {
                _isRunning = false;
                RaiseError(P2PErrorCode.ServerBindError, null, null, null, null,
                    $"서버 포트({ip}:{port}) 오픈 실패: 이미 다른 프로그램이 포트를 사용 중이거나 해당 IP에 바인딩할 수 없습니다. ({sockEx.Message})", sockEx);
                return false;
            }
            catch (Exception ex)
            {
                _isRunning = false;
                RaiseError(P2PErrorCode.ServerBindError, null, null, null, null,
                    $"서버 시작 중 알 수 없는 오류 발생: {ex.Message}", ex);
                return false;
            }
        }

        /// <summary>
        /// 송신자(서버) 포트를 닫고 진행 중인 모든 연결을 중단합니다.
        /// </summary>
        public void StopServer()
        {
            if (!_isRunning) return;
            _isRunning = false;

            try { _cts?.Cancel(); } catch { }
            try { _listener?.Stop(); } catch { }

            foreach (var kv in _activeSessions)
            {
                try { kv.Value.Cts?.Cancel(); } catch { }
                try { kv.Value.Client?.Close(); } catch { }
            }
            _activeSessions.Clear();

            lock (_queueLock)
            {
                _activeTransferCount = 0;
                while (_waitingQueue.Count > 0)
                {
                    _waitingQueue.Dequeue().TrySetCanceled();
                }
            }

            Log("[서버 중지] P2P 송신 서버가 중지되었습니다.");
        }

        /// <summary>
        /// 2. 전송할 파일과 채널ID(chId), 파일ID(msgId)를 서버에 등록합니다. (유효시간 기본 30분)
        /// 존재하지 않는 파일이나 잘못된 값을 입력해도 프로그램이 죽지 않고 오류 메시지를 출력한 뒤 null을 반환합니다.
        /// </summary>
        public RegisteredFileItem RegisterFile(string chId, string msgId, string filePath)
        {
            TryRegisterFile(chId, msgId, filePath, out RegisteredFileItem item, out _);
            return item;
        }

        /// <summary>
        /// 안전한 파일 등록 메서드 (성공 여부 bool 반환 및 실패 시 상세 errorMessage 반환)
        /// </summary>
        public bool TryRegisterFile(string chId, string msgId, string filePath, out RegisteredFileItem registeredItem, out string errorMessage)
        {
            registeredItem = null;
            errorMessage = null;

            if (string.IsNullOrWhiteSpace(chId))
            {
                chId = "DEFAULT_CH";
            }

            if (string.IsNullOrWhiteSpace(msgId))
            {
                errorMessage = "파일ID(msgID)가 비어 있습니다. 유효한 파일ID(msgID)를 입력해주세요.";
                RaiseError(P2PErrorCode.InvalidParameter, chId, msgId, null, null, errorMessage);
                return false;
            }

            if (string.IsNullOrWhiteSpace(filePath))
            {
                errorMessage = $"파일 경로가 입력되지 않았습니다. (chID:'{chId}', msgID:'{msgId}')";
                RaiseError(P2PErrorCode.InvalidParameter, chId, msgId, null, null, errorMessage);
                return false;
            }

            try
            {
                string cleanPath = filePath.Trim().Trim('"');
                if (cleanPath.IndexOfAny(Path.GetInvalidPathChars()) >= 0)
                {
                    errorMessage = $"파일 경로에 허용되지 않는 잘못된 문자가 포함되어 있습니다: '{filePath}'";
                    RaiseError(P2PErrorCode.InvalidParameter, chId, msgId, null, null, errorMessage);
                    return false;
                }

                if (!File.Exists(cleanPath))
                {
                    errorMessage = $"등록할 파일이 존재하지 않습니다. 경로를 확인해주세요: '{cleanPath}' (chID:'{chId}', msgID:'{msgId}')";
                    RaiseError(P2PErrorCode.SourceFileNotFound, chId, msgId, null, null, errorMessage);
                    return false;
                }

                var fi = new FileInfo(cleanPath);
                string key = BuildFileKey(chId, msgId);

                // (chId, msgId, filePath)가 모두 동일한 중복 아이템이 이미 등록되어 있는지 확인
                if (_registeredFiles.TryGetValue(key, out var existingItem))
                {
                    if (string.Equals(existingItem.LocalSourcePath, fi.FullName, StringComparison.OrdinalIgnoreCase) && !existingItem.IsExpired)
                    {
                        registeredItem = existingItem;
                        errorMessage = $"이미 동일한 파일(chID:'{chId.Trim()}', msgID:'{msgId.Trim()}', 경로:'{fi.FullName}')이 등록되어 있어 중복 등록을 건너뜁니다.";
                        Log("[중복 등록 무시] " + errorMessage);
                        return false;
                    }
                }

                // 파일 읽기 접근 권한 사전 테스트
                using (var testFs = new FileStream(fi.FullName, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                {
                }

                int currentChunkSize = ChunkSize > 0 ? ChunkSize : 64 * 1024;
                int totalChunks = (int)((fi.Length + currentChunkSize - 1) / currentChunkSize);
                if (totalChunks == 0) totalChunks = 1;

                var nowUtc = DateTime.UtcNow;
                registeredItem = new RegisteredFileItem
                {
                    ChId = chId.Trim(),
                    MsgId = msgId.Trim(),
                    FileName = fi.Name,
                    LocalSourcePath = fi.FullName,
                    FileSize = fi.Length,
                    ChunkSize = currentChunkSize,
                    TotalChunks = totalChunks,
                    CreatedUtcTicks = nowUtc.Ticks,
                    ExpireUtcTicks = nowUtc.Add(ValidDuration).Ticks
                };

                _registeredFiles[key] = registeredItem;
                Log($"[파일 등록 완료] chID='{registeredItem.ChId}', msgID='{registeredItem.MsgId}', 파일명='{registeredItem.FileName}', 크기={FormatBytes(registeredItem.FileSize)} (총 {registeredItem.TotalChunks}패킷, 유효시간:{registeredItem.RemainingValidTimeText})");
                return true;
            }
            catch (UnauthorizedAccessException authEx)
            {
                errorMessage = $"파일 읽기 권한이 없거나 접근이 거부되었습니다: '{filePath}' ({authEx.Message})";
                RaiseError(P2PErrorCode.DiskAccessError, chId, msgId, null, null, errorMessage, authEx);
                return false;
            }
            catch (IOException ioEx)
            {
                errorMessage = $"파일이 다른 프로그램에서 독점 사용 중이어서 읽을 수 없습니다: '{filePath}' ({ioEx.Message})";
                RaiseError(P2PErrorCode.DiskAccessError, chId, msgId, null, null, errorMessage, ioEx);
                return false;
            }
            catch (Exception ex)
            {
                errorMessage = $"파일 등록 중 오류 발생 ('{filePath}'): {ex.Message}";
                RaiseError(P2PErrorCode.InvalidParameter, chId, msgId, null, null, errorMessage, ex);
                return false;
            }
        }

        /// <summary>
        /// 등록했던 송신 파일(chId, msgId)을 서버 목록에서 등록 해제(Unregister)합니다.
        /// 만약 해당 파일을 현재 전송 중인 세션이 있다면 함께 취소합니다.
        /// </summary>
        public bool UnregisterFile(string chId, string msgId)
        {
            if (string.IsNullOrWhiteSpace(chId)) chId = "DEFAULT_CH";
            if (string.IsNullOrWhiteSpace(msgId))
            {
                Log("[등록 해제 실패] 파일ID(msgID)가 비어 있습니다.");
                return false;
            }

            string key = BuildFileKey(chId, msgId);
            if (_registeredFiles.TryRemove(key, out var removed))
            {
                // 진행 중인 전송이 있으면 함께 취소
                CancelTransfer(removed.ChId, removed.MsgId);

                // 상태 기록 맵에서도 정리
                foreach (var kv in _transferStatusMap.ToList())
                {
                    if (kv.Value.ChId == removed.ChId && kv.Value.MsgId == removed.MsgId)
                    {
                        _transferStatusMap.TryRemove(kv.Key, out _);
                    }
                }

                Log($"[파일 등록 해제(Unregister)] chID='{removed.ChId}', msgID='{removed.MsgId}', 파일명='{removed.FileName}' 등록 해제 완료.");
                return true;
            }
            else
            {
                Log($"[등록 해제 안내] 등록되어 있지 않은 파일입니다. (chID:'{chId.Trim()}', msgID:'{msgId.Trim()}')");
                return false;
            }
        }

        /// <summary>
        /// 등록된 모든 송신 파일을 일괄 등록 해제(UnregisterAll)합니다.
        /// </summary>
        public void UnregisterAllFiles()
        {
            foreach (var item in _registeredFiles.Values.ToList())
            {
                UnregisterFile(item.ChId, item.MsgId);
            }
            _registeredFiles.Clear();
            _transferStatusMap.Clear();
            Log("[전체 파일 등록 해제] 모든 송신 파일 등록이 초기화되었습니다.");
        }

        /// <summary>
        /// [송신 파일 목록 조회 1]
        /// 등록된 송신 파일들의 목록과 각 파일의 상태(Ready, Expired, FileMissing) 및 남은 유효시간(RemainingValidTimeText)을 조회합니다.
        /// </summary>
        public List<RegisteredFileItem> GetRegisteredFiles(string chId = null)
        {
            var query = _registeredFiles.Values.AsEnumerable();
            if (!string.IsNullOrWhiteSpace(chId))
            {
                query = query.Where(x => x.ChId == chId.Trim());
            }
            return query.OrderBy(x => x.CreatedUtcTicks).ToList();
        }

        /// <summary>
        /// [송신 전송 상태 목록 조회 2]
        /// 각 수신자(사번/이름)별로 현재 전송 중이거나 완료/취소/대기 중인 송신 상태와 진행률(%), 남은 유효시간을 조회합니다.
        /// </summary>
        public List<SenderTransferStatusItem> GetSenderTransferList(string chId = null, string msgId = null)
        {
            var query = _transferStatusMap.Values.AsEnumerable();
            if (!string.IsNullOrWhiteSpace(chId))
                query = query.Where(x => x.ChId == chId.Trim());
            if (!string.IsNullOrWhiteSpace(msgId))
                query = query.Where(x => x.MsgId == msgId.Trim());

            return query.ToList();
        }

        /// <summary>
        /// 송신자 측에서 특정 채널ID(chId) + 파일ID(msgId) 또는 특정 사번(peerId)의 전송을 취소합니다.
        /// </summary>
        public void CancelTransfer(string chId, string msgId, string peerId = null)
        {
            int count = 0;
            foreach (var kv in _activeSessions)
            {
                var s = kv.Value;
                if ((string.IsNullOrEmpty(chId) || s.ChId == chId) &&
                    (string.IsNullOrEmpty(msgId) || s.MsgId == msgId) &&
                    (string.IsNullOrEmpty(peerId) || s.PeerId == peerId))
                {
                    count++;
                    s.IsCancelled = true;
                    try
                    {
                        SendSessionControlAsync(s, P2PCommand.Cancel, s.LastChunkIndex, "송신자에 의해 취소됨", CancellationToken.None).Wait(150);
                    }
                    catch { }
                    try { s.Cts?.Cancel(); } catch { }

                    Log($"[송신 취소] chID='{s.ChId}', msgID='{s.MsgId}', 수신자={s.PeerName}({s.PeerId}) 전송 취소됨.");
                    if (s.FileItem != null)
                    {
                        RaiseProgress(s.SessionKey, s.FileItem, s.PeerId, s.PeerName, s.LastChunkIndex, TransferStatus.Cancelled, "송신자에 의해 취소됨");
                    }
                }
            }

            if (count == 0)
            {
                Log($"[송신 취소 안내] 현재 전송 중인 해당 항목(chID:'{chId}', msgID:'{msgId}')이 없습니다.");
            }
        }

        private async Task AcceptClientsLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested && _isRunning)
            {
                try
                {
                    TcpClient client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    _ = Task.Run(() => HandleClientSessionAsync(client, ct));
                }
                catch
                {
                    if (ct.IsCancellationRequested || !_isRunning) break;
                }
            }
        }

        private async Task HandleClientSessionAsync(TcpClient tcpClient, CancellationToken serverCt)
        {
            string sessionKey = Guid.NewGuid().ToString("N");
            string peerId = "";
            string peerName = "";
            string chId = "";
            string msgId = "";
            RegisteredFileItem fileItem = null;
            ClientSession session = null;
            bool slotAcquired = false;

            using (tcpClient)
            {
                tcpClient.NoDelay = true;
                var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(serverCt);
                CancellationToken ct = sessionCts.Token;

                try
                {
                    NetworkStream stream = tcpClient.GetStream();

                    // 1. 수신자로부터 Handshake 또는 QueryMetaOnly 패킷 수신
                    P2PFrame hsFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                    if (hsFrame == null || (hsFrame.Command != P2PCommand.Handshake && hsFrame.Command != P2PCommand.QueryMetaOnly))
                        return;

                    bool isMetaQueryOnly = (hsFrame.Command == P2PCommand.QueryMetaOnly);
                    var req = FrameStreamHelper.DeserializeJson<HandshakeRequest>(hsFrame.GetPayloadText());
                    if (req == null)
                    {
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Error, 0, "INVALID_PARAM:요청 데이터(Handshake)가 올바르지 않습니다.", ct).ConfigureAwait(false);
                        return;
                    }

                    peerId = string.IsNullOrWhiteSpace(req.PeerId) ? "UnknownID" : req.PeerId.Trim();
                    peerName = string.IsNullOrWhiteSpace(req.PeerName) ? "UnknownName" : req.PeerName.Trim();
                    chId = string.IsNullOrWhiteSpace(req.ChId) ? "DEFAULT_CH" : req.ChId.Trim();
                    msgId = (req.MsgId ?? "").Trim();
                    int startChunk = hsFrame.ChunkIndex;

                    if (!isMetaQueryOnly)
                    {
                        Log($"[수신자 요청 확인] 사번(peerID)='{peerId}', 이름(peerName)='{peerName}', 채널ID(chID)='{chId}', 파일ID(msgID)='{msgId}'");
                    }

                    // 2. 요청한 (chId + msgId) 파일이 RegisterFile로 등록되어 있는지 확인
                    string fileKey = BuildFileKey(chId, msgId);
                    if (!_registeredFiles.TryGetValue(fileKey, out fileItem))
                    {
                        string unregMsg = $"송신자 서버에 등록(RegisterFile)되지 않은 파일입니다. (요청한 chID:'{chId}', msgID:'{msgId}')";
                        RaiseError(P2PErrorCode.UnregisteredFileRequested, chId, msgId, peerId, peerName,
                            $"수신자 {peerName}({peerId})가 미등록 파일(chID:'{chId}', msgID:'{msgId}')을 요청하여 거부했습니다.");
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Error, 0, "UNREGISTERED:" + unregMsg, ct).ConfigureAwait(false);
                        return;
                    }

                    // 3. 30분 유효시간 만료 여부 확인
                    if (fileItem.IsExpired)
                    {
                        string expMsg = $"파일의 유효시간(30분)이 만료되어 전송할 수 없습니다. (chID:'{chId}', msgID:'{msgId}', 파일명:'{fileItem.FileName}')";
                        RaiseError(P2PErrorCode.FileExpired, chId, msgId, peerId, peerName, expMsg);
                        if (!isMetaQueryOnly)
                        {
                            RaiseProgress(sessionKey, fileItem, peerId, peerName, startChunk, TransferStatus.Expired, expMsg, true, P2PErrorCode.FileExpired);
                        }
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Error, 0, "EXPIRED:" + expMsg, ct).ConfigureAwait(false);
                        return;
                    }

                    // 4. 등록 당시에는 있었으나 현재 디스크에서 원본 파일이 삭제/이동된 경우
                    if (!File.Exists(fileItem.LocalSourcePath))
                    {
                        string missingMsg = $"송신자 PC에서 원본 파일('{fileItem.FileName}')을 찾을 수 없습니다. 파일이 삭제되었거나 이동되었습니다.";
                        RaiseError(P2PErrorCode.SourceFileNotFound, chId, msgId, peerId, peerName, missingMsg);
                        if (!isMetaQueryOnly)
                        {
                            RaiseProgress(sessionKey, fileItem, peerId, peerName, startChunk, TransferStatus.FileMissing, missingMsg, true, P2PErrorCode.SourceFileNotFound);
                        }
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Error, 0, "FILE_MISSING:" + missingMsg, ct).ConfigureAwait(false);
                        return;
                    }

                    // 5. 수신자에게 파일 메타데이터(HandshakeAck) 전송
                    var ackMeta = new HandshakeResponse
                    {
                        ChId = fileItem.ChId,
                        MsgId = fileItem.MsgId,
                        FileName = fileItem.FileName,
                        FileSize = fileItem.FileSize,
                        ChunkSize = fileItem.ChunkSize,
                        TotalChunks = fileItem.TotalChunks,
                        CreatedUtcTicks = fileItem.CreatedUtcTicks,
                        ExpireUtcTicks = fileItem.ExpireUtcTicks
                    };

                    if (isMetaQueryOnly)
                    {
                        // 사전 이어받기 진행률 조회용(QueryMetaOnly)이면 메타데이터만 응답하고 즉시 종료
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.HandshakeAck, 0, FrameStreamHelper.SerializeJson(ackMeta), ct).ConfigureAwait(false);
                        return;
                    }

                    session = new ClientSession
                    {
                        SessionKey = sessionKey,
                        PeerId = peerId,
                        PeerName = peerName,
                        ChId = chId,
                        MsgId = msgId,
                        FileItem = fileItem,
                        LastChunkIndex = startChunk,
                        Client = tcpClient,
                        Stream = stream,
                        Cts = sessionCts
                    };
                    _activeSessions[sessionKey] = session;

                    await SendSessionControlAsync(session, P2PCommand.HandshakeAck, startChunk, FrameStreamHelper.SerializeJson(ackMeta), ct).ConfigureAwait(false);

                    // 6. 동시 전송 제한 확인 (초과 시 WaitQueue 명령 보내고 슬롯이 빌 때까지 대기)
                    if (!TryAcquireSlot(out TaskCompletionSource<bool> waiterTcs))
                    {
                        Log($"[수신 대기열 진입] 사번:{peerId}({peerName}), chID:'{chId}', msgID:'{msgId}' - 동시 전송 제한({MaxConcurrentReceivers}개) 초과로 대기 중...");
                        RaiseProgress(sessionKey, fileItem, peerId, peerName, startChunk, TransferStatus.WaitingQueue, "동시 전송 제한 대기 중");
                        await SendSessionControlAsync(session, P2PCommand.WaitQueue, startChunk, "동시 전송 제한 대기 중...", ct).ConfigureAwait(false);

                        using (ct.Register(() => waiterTcs.TrySetCanceled()))
                        {
                            await waiterTcs.Task.ConfigureAwait(false);
                        }
                    }
                    slotAcquired = true;

                    // 7. 대기열 통과 -> QueueReady 전송
                    Log($"[전송 시작 승인] 사번:{peerId}({peerName}), chID:'{chId}', msgID:'{msgId}' ({fileItem.FileName}) 패킷 전송을 시작합니다.");
                    await SendSessionControlAsync(session, P2PCommand.QueueReady, startChunk, "READY", ct).ConfigureAwait(false);

                    // 8. 수신자가 한 패킷씩 번호(RequestChunk)를 요청하면 해당 패킷을 암호화+SHA256과 함께 전송
                    using (var fs = new FileStream(fileItem.LocalSourcePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        byte[] readBuf = new byte[fileItem.ChunkSize];

                        while (!ct.IsCancellationRequested && !session.IsCancelled)
                        {
                            P2PFrame reqFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                            if (reqFrame == null)
                            {
                                throw new IOException("수신자와의 네트워크 연결이 비정상적으로 끊어졌습니다.");
                            }

                            switch (reqFrame.Command)
                            {
                                case P2PCommand.RequestChunk:
                                    {
                                        int chunkIdx = reqFrame.ChunkIndex;
                                        if (chunkIdx < 0 || chunkIdx >= fileItem.TotalChunks)
                                        {
                                            await SendSessionControlAsync(session, P2PCommand.Error, chunkIdx, $"INVALID_CHUNK:잘못된 패킷 번호({chunkIdx}) 요청됨", ct).ConfigureAwait(false);
                                            break;
                                        }

                                        session.LastChunkIndex = chunkIdx;

                                        while (session.IsPaused && !ct.IsCancellationRequested && !session.IsCancelled)
                                        {
                                            await Task.Delay(100, ct).ConfigureAwait(false);
                                        }

                                        if (session.IsCancelled) break;

                                        long offset = (long)chunkIdx * fileItem.ChunkSize;
                                        fs.Seek(offset, SeekOrigin.Begin);

                                        int bytesRead = 0;
                                        if (fileItem.FileSize > 0)
                                        {
                                            bytesRead = await fs.ReadAsync(readBuf, 0, fileItem.ChunkSize, ct).ConfigureAwait(false);
                                        }

                                        // SHA-256 무결성 해시 및 XOR 조각 암호화
                                        byte[] sha256 = ChunkSecurityHelper.ComputeSha256(readBuf, 0, bytesRead);
                                        byte[] encrypted = ChunkSecurityHelper.TransformChunk(readBuf, bytesRead, chunkIdx);

                                        var sw = System.Diagnostics.Stopwatch.StartNew();

                                        await SendSessionFrameAsync(session, P2PCommand.DataChunk, chunkIdx, sha256, encrypted, ct).ConfigureAwait(false);

                                        // 속도 제한(KB/s) 적용
                                        int limit = SpeedLimitKbps;
                                        if (limit > 0 && encrypted.Length > 0)
                                        {
                                            double targetSec = (double)(encrypted.Length + FrameStreamHelper.HeaderSize) / (limit * 1024.0);
                                            double elapsedSec = sw.Elapsed.TotalSeconds;
                                            if (targetSec > elapsedSec)
                                            {
                                                int delayMs = (int)((targetSec - elapsedSec) * 1000);
                                                if (delayMs > 1) await Task.Delay(delayMs, ct).ConfigureAwait(false);
                                            }
                                        }

                                        int doneCount = chunkIdx + 1;
                                        session.LastChunkIndex = doneCount;
                                        double pct = fileItem.TotalChunks > 0 ? (doneCount * 100.0 / fileItem.TotalChunks) : 100.0;

                                        // 속도 제한 대기 도중에 Pause 명령이 들어온 경우 Transferring으로 덮어쓰지 않고 Paused 유지
                                        if (session.IsPaused)
                                        {
                                            RaiseProgress(sessionKey, fileItem, peerId, peerName, doneCount, TransferStatus.Paused, "일시정지됨", false);
                                            break;
                                        }

                                        // 0.5초(500ms, ProgressIntervalMs) 간격 또는 첫 패킷 / 마지막 패킷일 때만 이벤트 및 로그 업데이트
                                        long nowTicks = DateTime.UtcNow.Ticks;
                                        double elapsedMs = (nowTicks - session.LastProgressTick) / (double)TimeSpan.TicksPerMillisecond;
                                        bool shouldNotify = (doneCount == 1) || (doneCount == fileItem.TotalChunks) || (elapsedMs >= ProgressIntervalMs);

                                        if (shouldNotify)
                                        {
                                            session.LastProgressTick = nowTicks;
                                            Log($"[패킷 송신] -> {peerName}({peerId}) | chID:{chId} | msgID:{msgId} ({fileItem.FileName}) | 패킷 {doneCount}/{fileItem.TotalChunks} ({pct:0.0}%)");
                                        }

                                        RaiseProgress(sessionKey, fileItem, peerId, peerName, doneCount, TransferStatus.Transferring, $"전송 중 ({doneCount}/{fileItem.TotalChunks})", shouldNotify);
                                    }
                                    break;

                                case P2PCommand.Pause:
                                    session.IsPaused = true;
                                    Log($"[일시정지] 수신자 {peerName}({peerId}) | chID:{chId} | msgID:{msgId} 일시정지됨 (패킷 {reqFrame.ChunkIndex}/{fileItem.TotalChunks})");
                                    RaiseProgress(sessionKey, fileItem, peerId, peerName, reqFrame.ChunkIndex, TransferStatus.Paused, "일시정지됨");
                                    break;

                                case P2PCommand.Resume:
                                    session.IsPaused = false;
                                    Log($"[전송 재개] 수신자 {peerName}({peerId}) | chID:{chId} | msgID:{msgId} 전송 재개됨 (패킷 {reqFrame.ChunkIndex}번~)");
                                    RaiseProgress(sessionKey, fileItem, peerId, peerName, reqFrame.ChunkIndex, TransferStatus.Transferring, "전송 재개됨");
                                    break;

                                case P2PCommand.Cancel:
                                    session.IsCancelled = true;
                                    Log($"[수신자 취소] 수신자 {peerName}({peerId}) | chID:{chId} | msgID:{msgId} 전송을 취소했습니다.");
                                    RaiseProgress(sessionKey, fileItem, peerId, peerName, reqFrame.ChunkIndex, TransferStatus.Cancelled, "수신자가 취소함");
                                    return;

                                case P2PCommand.FileCompleted:
                                    Log($"[송신 완료] -> {peerName}({peerId}) | chID:{chId} | msgID:{msgId} ({fileItem.FileName}) 모든 패킷({fileItem.TotalChunks}개) 전송 및 병합 완료!");
                                    RaiseProgress(sessionKey, fileItem, peerId, peerName, fileItem.TotalChunks, TransferStatus.Completed, "전송 완료");
                                    return;
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (fileItem != null && (session == null || !session.IsCancelled))
                    {
                        int lastIdx = session != null ? session.LastChunkIndex : 0;
                        double savedPct = fileItem.TotalChunks > 0 ? (lastIdx * 100.0 / fileItem.TotalChunks) : 0.0;
                        string errMsg = $"송신 중 네트워크 연결이 끊어졌거나 오류가 발생했습니다: {ex.Message} (전송 완료된 진행률: {savedPct:0.0}%, {lastIdx}/{fileItem.TotalChunks} 패킷 - 재접속 시 이어보내기 가능)";
                        RaiseError(P2PErrorCode.NetworkDisconnected, chId, msgId, peerId, peerName, errMsg, ex);
                        RaiseProgress(sessionKey, fileItem, peerId, peerName, lastIdx, TransferStatus.Error, errMsg, true, P2PErrorCode.NetworkDisconnected);
                    }
                }
                finally
                {
                    _activeSessions.TryRemove(sessionKey, out _);
                    if (slotAcquired)
                    {
                        ReleaseSlot();
                    }
                }
            }
        }

        private bool TryAcquireSlot(out TaskCompletionSource<bool> waiterTcs)
        {
            waiterTcs = null;
            lock (_queueLock)
            {
                if (_activeTransferCount < _maxConcurrentReceivers && _waitingQueue.Count == 0)
                {
                    _activeTransferCount++;
                    return true;
                }

                waiterTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                _waitingQueue.Enqueue(waiterTcs);
                return false;
            }
        }

        private void ReleaseSlot()
        {
            lock (_queueLock)
            {
                if (_activeTransferCount > 0)
                    _activeTransferCount--;
            }
            ProcessWaitingQueue();
        }

        private void ProcessWaitingQueue()
        {
            lock (_queueLock)
            {
                while (_activeTransferCount < _maxConcurrentReceivers && _waitingQueue.Count > 0)
                {
                    var next = _waitingQueue.Dequeue();
                    if (next.TrySetResult(true))
                    {
                        _activeTransferCount++;
                    }
                }
            }
        }

        private void Log(string msg)
        {
            OnLog?.Invoke($"[{DateTime.Now:HH:mm:ss.fff}] {msg}");
        }

        private void RaiseError(P2PErrorCode code, string chId, string msgId, string peerId, string peerName, string errorMessage, Exception ex = null)
        {
            Log($"[오류 - {code}] {errorMessage}");
            OnError?.Invoke(this, new P2PErrorEventArgs
            {
                ErrorCode = code,
                ChId = chId,
                MsgId = msgId,
                PeerId = peerId,
                PeerName = peerName,
                ErrorMessage = errorMessage,
                Exception = ex
            });
        }

        private void RaiseProgress(string sessionKey, RegisteredFileItem item, string peerId, string peerName, int completedChunks, TransferStatus status, string message, bool fireEvent = true, P2PErrorCode errorCode = P2PErrorCode.None)
        {
            string mapKey = $"{item.ChId}_{item.MsgId}_{peerId}";
            _transferStatusMap[mapKey] = new SenderTransferStatusItem
            {
                SessionKey = sessionKey,
                ChId = item.ChId,
                MsgId = item.MsgId,
                FileName = item.FileName,
                FileSize = item.FileSize,
                PeerId = peerId,
                PeerName = peerName,
                CurrentChunkIndex = completedChunks,
                TotalChunks = item.TotalChunks,
                Status = status,
                ExpireTimeUtc = item.ExpireTimeUtc
            };

            if (!fireEvent) return;

            long transferred = Math.Min(item.FileSize, (long)completedChunks * item.ChunkSize);
            OnProgress?.Invoke(this, new TransferProgressEventArgs
            {
                SessionKey = sessionKey,
                ChId = item.ChId,
                MsgId = item.MsgId,
                FileName = item.FileName,
                PeerId = peerId,
                PeerName = peerName,
                CurrentChunkIndex = completedChunks,
                TotalChunks = item.TotalChunks,
                ChunkSize = item.ChunkSize,
                TransferredBytes = transferred,
                TotalBytes = item.FileSize,
                Status = status,
                ErrorCode = errorCode,
                ExpireTimeUtc = item.ExpireTimeUtc,
                Message = message
            });
        }

        public static string FormatBytes(long bytes)
        {
            string[] units = { "B", "KB", "MB", "GB" };
            double val = bytes;
            int idx = 0;
            while (val >= 1024 && idx < units.Length - 1)
            {
                val /= 1024;
                idx++;
            }
            return $"{val:0.##} {units[idx]}";
        }

        public void Dispose()
        {
            StopServer();
        }
    }
}
