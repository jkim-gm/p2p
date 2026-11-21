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
    /// P2P 송신자(A) 측 순수 P2P 파일 전송 엔진 클래스 (채팅 통신 분리됨)
    /// - P2P 전송 전용 TCP 포트 리스닝 및 세션 관리
    /// - 수신자가 요청한 패킷 번호(ChunkIndex)와 일정한 패킷 크기(ChunkSize)를 기반으로 전송 진행률 자동 계산
    /// - 헤더의 P2PCommand로 데이터 패킷(DataChunk)과 제어 명령(RequestChunk, Pause, Resume, Cancel, WaitQueue 등) 구분 처리
    /// - 동시 수신자 최대 수 제한 (예: 3명 전송 중이면 E, F는 WaitingQueue 상태)
    /// - 전송 속도 제한(KB/s) 실시간 반영
    /// - 파일 조각(Chunk) 암호화 및 SHA-256 무결성 해시 제공
    /// </summary>
    public class P2PSenderNode : IDisposable
    {
        public string NodeId { get; } = "SENDER_A";
        public string NodeName { get; set; } = "Sender(A)";
        public int P2PTransferPort { get; private set; }
        public bool IsRunning => _isP2PServerRunning;

        /// <summary>
        /// 고정 파일 조각 크기 (기본 64 KB).
        /// 64KB로 설정하여 제어 명령(Pause/Cancel 등)의 응답 지연을 최소화하고 패킷 번호만으로 진행 상태를 산출합니다.
        /// </summary>
        public int DefaultChunkSize { get; set; } = 64 * 1024;

        /// <summary>
        /// 파일 전송 유효 시간 (기본 30분)
        /// </summary>
        public TimeSpan OfferValidDuration { get; set; } = TimeSpan.FromMinutes(30);

        private int _maxConcurrentReceivers = 3;
        /// <summary>
        /// 동시에 파일을 수신할 수 있는 최대 클라이언트 수 (기본 3명)
        /// </summary>
        public int MaxConcurrentReceivers
        {
            get => _maxConcurrentReceivers;
            set
            {
                if (value < 1) value = 1;
                _maxConcurrentReceivers = value;
                ProcessWaitingReceiversQueue();
            }
        }

        private int _speedLimitKbps = 0;
        /// <summary>
        /// 전송 속도 제한 (KB/s). 0 이하이면 무제한.
        /// </summary>
        public int SpeedLimitKbps
        {
            get => _speedLimitKbps;
            set => _speedLimitKbps = Math.Max(0, value);
        }

        private TcpListener _p2pListener;
        private CancellationTokenSource _cts;
        private bool _isP2PServerRunning;

        // 등록된 파일 전송 묶음 목록 (Key: TransferId)
        private readonly ConcurrentDictionary<string, TransferOffer> _offers = new ConcurrentDictionary<string, TransferOffer>();

        // 취소/일시정지 상태 추적 (Key: "{TransferId}_{PeerId}" 또는 "{TransferId}")
        private readonly ConcurrentDictionary<string, bool> _cancelledKeys = new ConcurrentDictionary<string, bool>();
        private readonly ConcurrentDictionary<string, bool> _pausedKeys = new ConcurrentDictionary<string, bool>();

        // 활성 P2P 세션 추적 (Key: "{TransferId}_{PeerId}")
        private readonly ConcurrentDictionary<string, ActivePeerSession> _activeSessions = new ConcurrentDictionary<string, ActivePeerSession>();

        // 동시 수신 제한 슬롯 관리 (PeerId 단위)
        private readonly object _slotLock = new object();
        private readonly HashSet<string> _activeUploadingPeers = new HashSet<string>();
        private readonly Queue<WaitingPeerSlot> _waitingPeersQueue = new Queue<WaitingPeerSlot>();

        public event EventHandler<TransferProgressEventArgs> TransferProgressUpdated;

        private class ActivePeerSession
        {
            public string TransferId { get; set; }
            public string FileId { get; set; }
            public string PeerId { get; set; }
            public string PeerName { get; set; }
            public TcpClient Client { get; set; }
            public NetworkStream Stream { get; set; }
            public SemaphoreSlim WriteLock { get; } = new SemaphoreSlim(1, 1);
            public CancellationTokenSource SessionCts { get; set; }
            public int LastChunkIndex { get; set; }
        }

        private class WaitingPeerSlot
        {
            public string PeerId { get; set; }
            public string PeerName { get; set; }
            public string TransferId { get; set; }
            public string FileId { get; set; }
            public TaskCompletionSource<bool> SlotGrantedTcs { get; } = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        /// <summary>
        /// 전송할 파일들을 등록하고, 채팅으로 상대방에게 알려줄 메타데이터(파일명/크기/유효시간 등)를 생성합니다.
        /// </summary>
        public TransferOffer CreateTransferOffer(IEnumerable<string> filePaths, int p2pPort)
        {
            P2PTransferPort = p2pPort;
            var fileList = new List<FileItemMeta>();

            foreach (var path in filePaths)
            {
                var fi = new FileInfo(path);
                if (!fi.Exists) continue;

                int totalChunks = (int)((fi.Length + DefaultChunkSize - 1) / DefaultChunkSize);
                if (totalChunks == 0) totalChunks = 1; // 0바이트 파일 대응

                fileList.Add(new FileItemMeta
                {
                    FileId = Guid.NewGuid().ToString("N"),
                    FileName = fi.Name,
                    FileSize = fi.Length,
                    ChunkSize = DefaultChunkSize,
                    TotalChunks = totalChunks,
                    LocalSourcePath = fi.FullName
                });
            }

            if (fileList.Count == 0)
                throw new ArgumentException("전송할 유효한 파일이 없습니다.");

            var nowUtc = DateTime.UtcNow;
            var offer = new TransferOffer
            {
                TransferId = Guid.NewGuid().ToString("N"),
                SenderName = NodeName,
                P2PPort = p2pPort,
                ChunkSize = DefaultChunkSize,
                CreatedUtcTicks = nowUtc.Ticks,
                ExpireUtcTicks = nowUtc.Add(OfferValidDuration).Ticks,
                Files = fileList
            };

            _offers[offer.TransferId] = offer;
            return offer;
        }

        /// <summary>
        /// 수신자가 수신을 수락했을 때(또는 미리) P2P TCP 서버 포트를 오픈합니다.
        /// bindIp가 "127.0.0.1"이면 IPAddress.Loopback으로 바인딩하여 Windows 방화벽 팝업을 방지합니다.
        /// </summary>
        public void OpenP2PServer(int p2pPort, string bindIp = null)
        {
            if (_isP2PServerRunning && P2PTransferPort == p2pPort) return;

            StopP2PServer();

            P2PTransferPort = p2pPort;
            _cts = new CancellationTokenSource();

            IPAddress listenAddress = IPAddress.Any;
            if (!string.IsNullOrWhiteSpace(bindIp) &&
                (bindIp.Trim() == "127.0.0.1" || bindIp.Trim().Equals("localhost", StringComparison.OrdinalIgnoreCase)))
            {
                listenAddress = IPAddress.Loopback;
            }

            _p2pListener = new TcpListener(listenAddress, P2PTransferPort);
            _p2pListener.Start();
            _isP2PServerRunning = true;

            Task.Run(() => AcceptP2PClientsLoopAsync(_cts.Token));
        }

        /// <summary>
        /// P2P 서버를 중지하고 모든 연결된 수신자 P2P 세션에 연결 종료를 알립니다. (네트워크 끊김 시뮬레이션 포함)
        /// </summary>
        public void StopP2PServer()
        {
            _isP2PServerRunning = false;

            // 활성 세션에 Disconnect 프레임 전송 시도 후 소켓 닫기
            foreach (var kv in _activeSessions)
            {
                var session = kv.Value;
                try
                {
                    if (session.Stream != null && session.Client != null && session.Client.Connected)
                    {
                        FrameStreamHelper.WriteControlFrameAsync(session.Stream, P2PCommand.Disconnect, session.LastChunkIndex, "SENDER_OFFLINE", CancellationToken.None).Wait(100);
                    }
                }
                catch { }
                try { session.SessionCts?.Cancel(); } catch { }
                try { session.Client?.Close(); } catch { }
            }
            _activeSessions.Clear();

            try { _cts?.Cancel(); } catch { }
            try { _p2pListener?.Stop(); } catch { }

            lock (_slotLock)
            {
                _activeUploadingPeers.Clear();
                while (_waitingPeersQueue.Count > 0)
                {
                    var waiter = _waitingPeersQueue.Dequeue();
                    waiter.SlotGrantedTcs.TrySetCanceled();
                }
            }
        }

        private async Task AcceptP2PClientsLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested && _isP2PServerRunning)
            {
                try
                {
                    TcpClient p2pClient = await _p2pListener.AcceptTcpClientAsync().ConfigureAwait(false);
                    _ = Task.Run(() => HandleP2PSessionAsync(p2pClient, ct));
                }
                catch
                {
                    if (ct.IsCancellationRequested || !_isP2PServerRunning) break;
                }
            }
        }

        /// <summary>
        /// 단일 P2P 포트에서 제어 명령(Handshake, RequestChunk, Pause, Resume, Cancel, FileCompleted) 및
        /// 데이터 패킷(DataChunk)을 주고받는 핵심 P2P 세션 루프
        /// </summary>
        private async Task HandleP2PSessionAsync(TcpClient p2pClient, CancellationToken serverCt)
        {
            string peerId = null;
            string peerName = null;
            string transferId = null;
            string fileId = null;
            string sessionKey = null;
            FileItemMeta fileMeta = null;
            TransferOffer offer = null;

            using (p2pClient)
            {
                p2pClient.NoDelay = true;
                var sessionCts = CancellationTokenSource.CreateLinkedTokenSource(serverCt);
                CancellationToken ct = sessionCts.Token;

                try
                {
                    NetworkStream stream = p2pClient.GetStream();

                    // 1. 첫 프레임: Handshake 제어 명령 수신
                    P2PFrame firstFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                    if (firstFrame == null || firstFrame.Command != P2PCommand.Handshake)
                        return;

                    var handshake = FrameStreamHelper.DeserializeJson<HandshakePayload>(firstFrame.GetPayloadText());
                    if (handshake == null) return;

                    peerId = handshake.PeerId;
                    peerName = handshake.PeerName;
                    transferId = handshake.TransferId;
                    fileId = handshake.FileId;
                    int initialChunk = Math.Max(0, firstFrame.ChunkIndex);

                    if (!_offers.TryGetValue(transferId, out offer))
                    {
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Error, 0, "OFFER_NOT_FOUND", ct).ConfigureAwait(false);
                        return;
                    }

                    // 30분 유효시간 체크
                    if (offer.IsExpired)
                    {
                        foreach (var f in offer.Files)
                        {
                            RaiseProgress(transferId, f.FileId, f.FileName, peerId, peerName, 0, f.TotalChunks, f.ChunkSize, f.FileSize, TransferStatus.Expired, "유효시간(30분) 만료됨");
                        }
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Error, 0, "EXPIRED", ct).ConfigureAwait(false);
                        return;
                    }

                    if (IsCancelled(transferId, peerId))
                    {
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Cancel, 0, "CANCELLED", ct).ConfigureAwait(false);
                        return;
                    }

                    fileMeta = offer.Files.FirstOrDefault(f => f.FileId == fileId);
                    if (fileMeta == null || !File.Exists(fileMeta.LocalSourcePath))
                    {
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Error, 0, "FILE_MISSING", ct).ConfigureAwait(false);
                        return;
                    }

                    sessionKey = $"{transferId}_{peerId}";
                    var activeSession = new ActivePeerSession
                    {
                        TransferId = transferId,
                        FileId = fileId,
                        PeerId = peerId,
                        PeerName = peerName,
                        Client = p2pClient,
                        Stream = stream,
                        SessionCts = sessionCts,
                        LastChunkIndex = initialChunk
                    };
                    _activeSessions[sessionKey] = activeSession;

                    // 2. 동시 수신자 수 제한 체크 (예: 최대 3명. 초과 시 대기열 진입 및 WaitQueue 제어 명령 전송)
                    bool acquiredImmediately = TryAcquirePeerSlot(peerId, peerName, transferId, fileId, out WaitingPeerSlot waiter);
                    if (!acquiredImmediately)
                    {
                        RaiseProgress(transferId, fileMeta.FileId, fileMeta.FileName, peerId, peerName,
                            initialChunk, fileMeta.TotalChunks, fileMeta.ChunkSize, fileMeta.FileSize,
                            TransferStatus.WaitingQueue, $"동시 전송 제한({MaxConcurrentReceivers}명) 대기 중...");

                        await SendSessionFrameAsync(activeSession, P2PCommand.WaitQueue, initialChunk, null, null, ct).ConfigureAwait(false);

                        using (ct.Register(() => waiter.SlotGrantedTcs.TrySetCanceled()))
                        {
                            await waiter.SlotGrantedTcs.Task.ConfigureAwait(false);
                        }
                    }

                    if (IsCancelled(transferId, peerId))
                    {
                        await SendSessionFrameAsync(activeSession, P2PCommand.Cancel, initialChunk, null, null, ct).ConfigureAwait(false);
                        return;
                    }

                    // 3. 대기열 통과 -> QueueReady 제어 명령 전송
                    await SendSessionFrameAsync(activeSession, P2PCommand.QueueReady, initialChunk, null, null, ct).ConfigureAwait(false);

                    // 4. 파일 스트림을 열고 수신자가 보내는 제어/요청 명령(RequestChunk, Pause, Resume, Cancel, FileCompleted)을 처리
                    using (var fs = new FileStream(fileMeta.LocalSourcePath, FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        byte[] readBuf = new byte[fileMeta.ChunkSize];

                        while (!ct.IsCancellationRequested)
                        {
                            P2PFrame reqFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                            if (reqFrame == null)
                            {
                                throw new IOException("수신자와의 P2P 연결이 종료되었습니다.");
                            }

                            if (IsCancelled(transferId, peerId))
                            {
                                await SendSessionFrameAsync(activeSession, P2PCommand.Cancel, activeSession.LastChunkIndex, null, null, ct).ConfigureAwait(false);
                                throw new OperationCanceledException("Transfer cancelled.");
                            }

                            switch (reqFrame.Command)
                            {
                                case P2PCommand.RequestChunk:
                                    {
                                        int requestedChunkIdx = reqFrame.ChunkIndex;
                                        activeSession.LastChunkIndex = requestedChunkIdx;

                                        // 송신자 측에서 일시정지(Pause)를 걸어둔 경우 재개될 때까지 대기
                                        while (IsPaused(transferId, peerId) && !ct.IsCancellationRequested && !IsCancelled(transferId, peerId))
                                        {
                                            await Task.Delay(100, ct).ConfigureAwait(false);
                                        }

                                        if (requestedChunkIdx < 0 || requestedChunkIdx >= fileMeta.TotalChunks)
                                        {
                                            await SendSessionFrameAsync(activeSession, P2PCommand.Error, requestedChunkIdx, null, null, ct).ConfigureAwait(false);
                                            break;
                                        }

                                        long offset = (long)requestedChunkIdx * fileMeta.ChunkSize;
                                        fs.Seek(offset, SeekOrigin.Begin);

                                        int bytesRead = 0;
                                        if (fileMeta.FileSize > 0)
                                        {
                                            bytesRead = await fs.ReadAsync(readBuf, 0, fileMeta.ChunkSize, ct).ConfigureAwait(false);
                                        }

                                        // 원본 조각의 SHA-256 무결성 해시 (32 bytes)
                                        byte[] sha256 = ChunkSecurityHelper.ComputeSha256(readBuf, 0, bytesRead);

                                        // 조각 간단 암호화 (XOR 스트림 변환)
                                        byte[] encryptedChunk = ChunkSecurityHelper.TransformChunk(readBuf, bytesRead, requestedChunkIdx);

                                        var sw = System.Diagnostics.Stopwatch.StartNew();

                                        // DataChunk 패킷 전송 (헤더에 Command=DataChunk, ChunkIndex, PayloadLen, SHA256 포함)
                                        await SendSessionFrameAsync(activeSession, P2PCommand.DataChunk, requestedChunkIdx, sha256, encryptedChunk, ct).ConfigureAwait(false);

                                        // 전송 속도 제어 (SpeedLimitKbps 옵션 적용)
                                        int currentSpeedLimit = SpeedLimitKbps;
                                        if (currentSpeedLimit > 0 && encryptedChunk.Length > 0)
                                        {
                                            double targetSeconds = (double)(encryptedChunk.Length + FrameStreamHelper.HeaderSize) / (currentSpeedLimit * 1024.0);
                                            double elapsedSeconds = sw.Elapsed.TotalSeconds;
                                            if (targetSeconds > elapsedSeconds)
                                            {
                                                int delayMs = (int)((targetSeconds - elapsedSeconds) * 1000);
                                                if (delayMs > 1)
                                                {
                                                    await Task.Delay(delayMs, ct).ConfigureAwait(false);
                                                }
                                            }
                                        }

                                        // 수신자가 요청한 패킷 번호(requestedChunkIdx + 1)와 고정 패킷 크기(fileMeta.ChunkSize)로 전송 상태 산출
                                        RaiseProgress(transferId, fileMeta.FileId, fileMeta.FileName, peerId, peerName,
                                            requestedChunkIdx + 1, fileMeta.TotalChunks, fileMeta.ChunkSize, fileMeta.FileSize,
                                            TransferStatus.Transferring, $"전송 중 ({requestedChunkIdx + 1}/{fileMeta.TotalChunks} 패킷)");
                                    }
                                    break;

                                case P2PCommand.Pause:
                                    {
                                        _pausedKeys[sessionKey] = true;
                                        RaiseProgress(transferId, fileMeta.FileId, fileMeta.FileName, peerId, peerName,
                                            reqFrame.ChunkIndex, fileMeta.TotalChunks, fileMeta.ChunkSize, fileMeta.FileSize,
                                            TransferStatus.Paused, $"수신자가 일시정지함 ({reqFrame.ChunkIndex}/{fileMeta.TotalChunks} 패킷)");
                                    }
                                    break;

                                case P2PCommand.Resume:
                                    {
                                        _pausedKeys.TryRemove(sessionKey, out _);
                                        RaiseProgress(transferId, fileMeta.FileId, fileMeta.FileName, peerId, peerName,
                                            reqFrame.ChunkIndex, fileMeta.TotalChunks, fileMeta.ChunkSize, fileMeta.FileSize,
                                            TransferStatus.Transferring, $"전송 재개됨 ({reqFrame.ChunkIndex}/{fileMeta.TotalChunks} 패킷~)");
                                    }
                                    break;

                                case P2PCommand.Cancel:
                                    {
                                        _cancelledKeys[sessionKey] = true;
                                        RaiseProgress(transferId, fileMeta.FileId, fileMeta.FileName, peerId, peerName,
                                            reqFrame.ChunkIndex, fileMeta.TotalChunks, fileMeta.ChunkSize, fileMeta.FileSize,
                                            TransferStatus.Cancelled, "수신자가 전송을 취소함");
                                        ReleasePeerSlot(peerId);
                                        return;
                                    }

                                case P2PCommand.FileCompleted:
                                    {
                                        RaiseProgress(transferId, fileMeta.FileId, fileMeta.FileName, peerId, peerName,
                                            fileMeta.TotalChunks, fileMeta.TotalChunks, fileMeta.ChunkSize, fileMeta.FileSize,
                                            TransferStatus.Completed, "전송 완료");

                                        // 해당 수신자의 이번 Offer 내 마지막 파일까지 완료되었으면 동시전송 슬롯 반환
                                        if (offer.Files.Last().FileId == fileMeta.FileId)
                                        {
                                            ReleasePeerSlot(peerId);
                                        }
                                        return;
                                    }
                            }
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    if (fileMeta != null && peerId != null)
                    {
                        bool isUserCancel = IsCancelled(transferId, peerId);
                        int lastChunk = _activeSessions.TryGetValue(sessionKey ?? "", out var s) ? s.LastChunkIndex : 0;
                        RaiseProgress(transferId, fileMeta.FileId, fileMeta.FileName, peerId, peerName ?? peerId,
                            lastChunk, fileMeta.TotalChunks, fileMeta.ChunkSize, fileMeta.FileSize,
                            isUserCancel ? TransferStatus.Cancelled : TransferStatus.Error,
                            isUserCancel ? "전송 취소됨" : $"연결 중단 ({lastChunk}/{fileMeta.TotalChunks} 패킷 완료 - 재접속 시 이어받기)");
                    }
                    if (peerId != null) ReleasePeerSlot(peerId);
                }
                catch (Exception ex)
                {
                    if (fileMeta != null && peerId != null)
                    {
                        bool isUserCancel = IsCancelled(transferId, peerId);
                        int lastChunk = _activeSessions.TryGetValue(sessionKey ?? "", out var s) ? s.LastChunkIndex : 0;
                        RaiseProgress(transferId, fileMeta.FileId, fileMeta.FileName, peerId, peerName ?? peerId,
                            lastChunk, fileMeta.TotalChunks, fileMeta.ChunkSize, fileMeta.FileSize,
                            isUserCancel ? TransferStatus.Cancelled : TransferStatus.Error,
                            isUserCancel ? "전송 취소됨" : $"오류 중단: {ex.Message} ({lastChunk}/{fileMeta.TotalChunks} 패킷 완료 - 이어받기 대기)");
                    }
                    if (peerId != null) ReleasePeerSlot(peerId);
                }
                finally
                {
                    if (sessionKey != null)
                    {
                        _activeSessions.TryRemove(sessionKey, out _);
                    }
                }
            }
        }

        private async Task SendSessionFrameAsync(ActivePeerSession session, P2PCommand cmd, int chunkIndex, byte[] hash32, byte[] payload, CancellationToken ct)
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

        /// <summary>
        /// 송신자 측에서 특정 수신자(또는 전체) 전송을 일시정지(Pause) 제어 명령으로 보냅니다.
        /// </summary>
        public async Task PauseTransferAsync(string transferId, string targetPeerId)
        {
            string key = $"{transferId}_{targetPeerId}";
            _pausedKeys[key] = true;

            if (_activeSessions.TryGetValue(key, out var session))
            {
                try
                {
                    await SendSessionFrameAsync(session, P2PCommand.Pause, session.LastChunkIndex, null, null, CancellationToken.None).ConfigureAwait(false);
                }
                catch { }

                if (_offers.TryGetValue(transferId, out var offer))
                {
                    var f = offer.Files.FirstOrDefault(x => x.FileId == session.FileId);
                    if (f != null)
                    {
                        RaiseProgress(transferId, f.FileId, f.FileName, session.PeerId, session.PeerName,
                            session.LastChunkIndex, f.TotalChunks, f.ChunkSize, f.FileSize, TransferStatus.Paused, "송신자가 일시정지함");
                    }
                }
            }
        }

        /// <summary>
        /// 송신자 측에서 일시정지된 전송을 재개(Resume) 제어 명령으로 보냅니다.
        /// </summary>
        public async Task ResumeTransferAsync(string transferId, string targetPeerId)
        {
            string key = $"{transferId}_{targetPeerId}";
            _pausedKeys.TryRemove(key, out _);

            if (_activeSessions.TryGetValue(key, out var session))
            {
                try
                {
                    await SendSessionFrameAsync(session, P2PCommand.Resume, session.LastChunkIndex, null, null, CancellationToken.None).ConfigureAwait(false);
                }
                catch { }
            }
        }

        /// <summary>
        /// 송신자가 특정 수신자(또는 전체 수신자)에 대한 전송을 취소합니다.
        /// P2P 연결이 수립된 상태라면 P2P 제어 명령(Cancel)을 즉시 전송하고, 연결 수립 전이라도 취소 상태로 등록합니다.
        /// </summary>
        public async Task CancelTransferAsync(string transferId, string targetPeerId = null, string targetPeerName = null)
        {
            var targetSessions = string.IsNullOrEmpty(targetPeerId)
                ? _activeSessions.Values.Where(s => s.TransferId == transferId).ToList()
                : _activeSessions.Values.Where(s => s.TransferId == transferId && s.PeerId == targetPeerId).ToList();

            if (string.IsNullOrEmpty(targetPeerId))
            {
                _cancelledKeys[transferId] = true;
            }
            else
            {
                _cancelledKeys[$"{transferId}_{targetPeerId}"] = true;
                ReleasePeerSlot(targetPeerId);
            }

            foreach (var session in targetSessions)
            {
                try
                {
                    await SendSessionFrameAsync(session, P2PCommand.Cancel, session.LastChunkIndex, null, null, CancellationToken.None).ConfigureAwait(false);
                }
                catch { }
                try { session.SessionCts?.Cancel(); } catch { }
                ReleasePeerSlot(session.PeerId);

                if (_offers.TryGetValue(transferId, out var offer))
                {
                    foreach (var f in offer.Files)
                    {
                        RaiseProgress(transferId, f.FileId, f.FileName, session.PeerId, session.PeerName,
                            0, f.TotalChunks, f.ChunkSize, f.FileSize, TransferStatus.Cancelled, "송신자에 의해 취소됨");
                    }
                }
            }

            // 아직 P2P 연결을 시작하지 않은(PendingAccept) 수신자에 대해서도 UI 상태가 즉시 취소됨으로 반영되도록 이벤트 발생
            if (!string.IsNullOrEmpty(targetPeerId) && targetSessions.Count == 0 && _offers.TryGetValue(transferId, out var pendingOffer))
            {
                foreach (var f in pendingOffer.Files)
                {
                    RaiseProgress(transferId, f.FileId, f.FileName, targetPeerId, targetPeerName ?? targetPeerId,
                        0, f.TotalChunks, f.ChunkSize, f.FileSize, TransferStatus.Cancelled, "송신자에 의해 취소됨 (연결 전 취소)");
                }
            }
        }

        /// <summary>
        /// 수신자가 P2P 연결(수신 시작)을 누르기 전에 채팅 메시지로 수신 거절/취소를 알려왔을 때 호출합니다.
        /// </summary>
        public void NotifyCancelledByPeerBeforeConnection(string transferId, string peerId, string peerName)
        {
            string key = $"{transferId}_{peerId}";
            _cancelledKeys[key] = true;
            ReleasePeerSlot(peerId);

            if (_activeSessions.TryGetValue(key, out var session))
            {
                try { session.SessionCts?.Cancel(); } catch { }
            }

            if (_offers.TryGetValue(transferId, out var offer))
            {
                foreach (var f in offer.Files)
                {
                    RaiseProgress(transferId, f.FileId, f.FileName, peerId, peerName ?? peerId,
                        0, f.TotalChunks, f.ChunkSize, f.FileSize, TransferStatus.Cancelled, "수신자가 수신 시작 전 취소함");
                }
            }
        }

        private bool TryAcquirePeerSlot(string peerId, string peerName, string transferId, string fileId, out WaitingPeerSlot waiter)
        {
            waiter = null;
            lock (_slotLock)
            {
                if (_activeUploadingPeers.Contains(peerId))
                {
                    return true;
                }

                if (_activeUploadingPeers.Count < _maxConcurrentReceivers && _waitingPeersQueue.Count == 0)
                {
                    _activeUploadingPeers.Add(peerId);
                    return true;
                }

                waiter = new WaitingPeerSlot
                {
                    PeerId = peerId,
                    PeerName = peerName,
                    TransferId = transferId,
                    FileId = fileId
                };
                _waitingPeersQueue.Enqueue(waiter);
                return false;
            }
        }

        private void ReleasePeerSlot(string peerId)
        {
            lock (_slotLock)
            {
                _activeUploadingPeers.Remove(peerId);
            }
            ProcessWaitingReceiversQueue();
        }

        private void ProcessWaitingReceiversQueue()
        {
            lock (_slotLock)
            {
                while (_activeUploadingPeers.Count < _maxConcurrentReceivers && _waitingPeersQueue.Count > 0)
                {
                    var nextWaiter = _waitingPeersQueue.Dequeue();
                    if (IsCancelled(nextWaiter.TransferId, nextWaiter.PeerId))
                    {
                        nextWaiter.SlotGrantedTcs.TrySetCanceled();
                        continue;
                    }

                    _activeUploadingPeers.Add(nextWaiter.PeerId);
                    nextWaiter.SlotGrantedTcs.TrySetResult(true);
                }
            }
        }

        private bool IsCancelled(string transferId, string peerId)
        {
            return _cancelledKeys.ContainsKey(transferId) || _cancelledKeys.ContainsKey($"{transferId}_{peerId}");
        }

        private bool IsPaused(string transferId, string peerId)
        {
            return _pausedKeys.ContainsKey(transferId) || _pausedKeys.ContainsKey($"{transferId}_{peerId}");
        }

        private void RaiseProgress(string transferId, string fileId, string fileName, string peerId, string peerName,
            int completedChunkCount, int totalChunks, int chunkSize, long totalBytes, TransferStatus status, string message)
        {
            // 패킷 크기(chunkSize)가 일정하므로 완료된 패킷 번호(completedChunkCount)만으로 전송된 바이트와 진행률을 계산
            long transferredBytes = Math.Min(totalBytes, (long)completedChunkCount * chunkSize);

            TransferProgressUpdated?.Invoke(this, new TransferProgressEventArgs
            {
                TransferId = transferId,
                FileId = fileId,
                FileName = fileName,
                PeerId = peerId,
                PeerName = peerName,
                CurrentChunkIndex = completedChunkCount,
                TotalChunks = totalChunks,
                ChunkSize = chunkSize,
                TransferredBytes = transferredBytes,
                TotalBytes = totalBytes,
                Status = status,
                Message = message
            });
        }

        public static string FormatBytes(long bytes)
        {
            string[] units = { "B", "KB", "MB", "GB", "TB" };
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
            StopP2PServer();
        }
    }

    [Serializable]
    public class HandshakePayload
    {
        public string TransferId { get; set; }
        public string FileId { get; set; }
        public string PeerId { get; set; }
        public string PeerName { get; set; }
    }
}
