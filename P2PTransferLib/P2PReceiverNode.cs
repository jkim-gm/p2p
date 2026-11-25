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
    /// 개별 수신 작업(Receiver 인스턴스 1개)의 상태, 유효시간 및 제어 정보를 담는 클래스
    /// </summary>
    public class P2PReceiverItem
    {
        public string TaskKey { get; set; }
        public string ServerIp { get; set; }
        public int ServerPort { get; set; }
        public string PeerId { get; set; }      // 사번
        public string PeerName { get; set; }    // 이름
        public string ChId { get; set; }        // 채널ID (chID)
        public string MsgId { get; set; }       // 파일ID (msgID)
        public string FileName { get; set; }
        public string DownloadFolder { get; set; }
        public long FileSize { get; set; }
        public int CurrentChunkIndex { get; set; }
        public int TotalChunks { get; set; }
        public double ProgressPercentage => TotalChunks > 0 ? Math.Min(100.0, CurrentChunkIndex * 100.0 / TotalChunks) : 0.0;
        public TransferStatus Status { get; set; }
        public P2PErrorCode LastErrorCode { get; set; }
        public string LastErrorMessage { get; set; }
        public string SavedFilePath { get; set; }
        public string PartFilePath { get; set; }
        public DateTime StartedTime { get; set; }
        public DateTime ExpireTimeUtc { get; set; }
        public bool IsMissingNotified { get; set; }
        public bool IsExpiredNotified { get; set; }

        /// <summary>
        /// 30분 유효시간이 만료되었는지 여부
        /// </summary>
        public bool IsExpired => ExpireTimeUtc != DateTime.MinValue && DateTime.UtcNow > ExpireTimeUtc;

        /// <summary>
        /// 남은 유효시간 (TimeSpan)
        /// </summary>
        public TimeSpan RemainingValidTime
        {
            get
            {
                if (ExpireTimeUtc == DateTime.MinValue) return TimeSpan.Zero;
                var rem = ExpireTimeUtc - DateTime.UtcNow;
                return rem.TotalSeconds > 0 ? rem : TimeSpan.Zero;
            }
        }

        /// <summary>
        /// 남은 유효시간 표시 문자열 (예: "29:45 남음", "전송완료", "00:00 (만료됨)")
        /// </summary>
        public string RemainingValidTimeText
        {
            get
            {
                if (Status == TransferStatus.Completed) return "전송완료";
                if (Status == TransferStatus.FileMissing) return "파일없음";
                if (Status == TransferStatus.Cancelled) return "취소됨";
                if (ExpireTimeUtc == DateTime.MinValue) return "확인중";
                if (IsExpired) return "00:00 (만료됨)";
                var rem = RemainingValidTime;
                return $"{(int)rem.TotalMinutes:D2}:{rem.Seconds:D2} 남음";
            }
        }

        /// <summary>
        /// 다운로드 완료된 파일이 디스크에서 삭제/이동되었는지 및 유효시간이 만료되었는지 최신 상태로 갱신합니다.
        /// </summary>
        public void RefreshStatus()
        {
            if (Status == TransferStatus.Completed && !string.IsNullOrEmpty(SavedFilePath))
            {
                if (!File.Exists(SavedFilePath))
                {
                    Status = TransferStatus.FileMissing;
                }
            }

            if (Status != TransferStatus.Completed && Status != TransferStatus.FileMissing &&
                Status != TransferStatus.Cancelled && Status != TransferStatus.Transferring &&
                IsExpired)
            {
                Status = TransferStatus.Expired;
            }
        }

        internal NetworkStream Stream { get; set; }
        internal SemaphoreSlim WriteLock { get; } = new SemaphoreSlim(1, 1);
        internal CancellationTokenSource Cts { get; set; }
        internal long LastProgressTick { get; set; }
        internal bool IsPaused { get; set; }
        internal bool IsCancelled { get; set; }
    }

    /// <summary>
    /// P2P 수신자(클라이언트) 자동 리스트 관리 모듈
    /// - StartDownloadAsync()를 호출할 때마다 P2PReceiverItem 객체가 생성되어 내부 리스트(_receiverMap)에 자동 등록됩니다.
    /// - CheckPartialFileProgress() 또는 CheckPartialProgressFromServerAsync()를 통해 기존에 받다 중지된 파일이 몇 퍼센트 받아졌는지 사전 확인할 수 있습니다.
    /// - 미등록 파일 요청, 네트워크 접속 불가, 타임아웃, 유효시간 만료, 디스크 권한 오류 등 모든 오류에 대해 상세 메시지를 제공합니다.
    /// </summary>
    public class P2PReceiverNode : IDisposable
    {
        /// <summary>
        /// 어디서든 접근 가능한 글로벌 수신 매니저 싱글톤 인스턴스
        /// </summary>
        public static P2PReceiverNode Instance { get; } = new P2PReceiverNode();

        /// <summary>
        /// 수신 중(Transferring)일 때 OnProgress 이벤트 및 로그를 발생시키는 최소 시간 간격 (밀리초 단위, 기본값: 500ms = 0.5초)
        /// </summary>
        public double ProgressIntervalMs { get; set; } = 500.0;

        /// <summary>
        /// 서버 접속 타임아웃 (밀리초 단위, 기본 5000ms = 5초)
        /// </summary>
        public int ConnectTimeoutMs { get; set; } = 5000;

        // 여러 개의 수신 작업(Receiver)을 자동으로 관리하는 스레드 안전 컬렉션 (Key: "{peerId}_{chId}_{msgId}_{folder}")
        private readonly ConcurrentDictionary<string, P2PReceiverItem> _receiverMap = new ConcurrentDictionary<string, P2PReceiverItem>();
        private readonly object _fileNameResolveLock = new object();

        /// <summary>
        /// 단순 로그 출력 이벤트
        /// </summary>
        public event Action<string> OnLog;

        /// <summary>
        /// 진행 상태 및 완료/취소/오류/파일없음(FileMissing) 이벤트
        /// </summary>
        public event EventHandler<TransferProgressEventArgs> OnProgress;

        /// <summary>
        /// 상세 오류 이벤트 (미등록 파일 요청, 네트워크 접속 실패, 잘못된 파라미터, 무결성 오류 등 발생 시 호출됨)
        /// </summary>
        public event EventHandler<P2PErrorEventArgs> OnError;

        /// <summary>
        /// [수신 파일 목록 조회 1]
        /// 현재 등록되어 관리 중인 모든 Receiver 목록을 리스트로 반환합니다.
        /// 조회 시 자동으로 완료된 파일의 디스크 존재 여부('파일없음')와 유효시간(30분) 만료 상태를 최신으로 갱신하여 반환합니다.
        /// </summary>
        public List<P2PReceiverItem> GetReceiverList(string chId = null)
        {
            var list = _receiverMap.Values.OrderBy(x => x.StartedTime).ToList();
            foreach (var item in list)
            {
                item.RefreshStatus();
            }

            if (!string.IsNullOrWhiteSpace(chId))
            {
                list = list.Where(x => x.ChId == chId.Trim()).ToList();
            }
            return list;
        }

        /// <summary>
        /// [수신 파일 목록 조회 2]
        /// 현재 전송(또는 대기/일시정지) 진행 중인 활성 Receiver 목록만 리스트로 반환합니다.
        /// </summary>
        public List<P2PReceiverItem> GetActiveReceiverList(string chId = null)
        {
            return GetReceiverList(chId)
                .Where(x => x.Status == TransferStatus.WaitingQueue || x.Status == TransferStatus.Transferring || x.Status == TransferStatus.Paused || x.Status == TransferStatus.Merging)
                .ToList();
        }

        /// <summary>
        /// 특정 (chId, msgId, peerId) 수신 항목을 목록에서 등록 해제(Unregister)합니다.
        /// 진행 중인 경우 취소 후 목록에서 제거합니다.
        /// </summary>
        public bool UnregisterReceiver(string chId, string msgId, string peerId = null)
        {
            CancelDownload(chId, msgId, peerId);

            int removedCount = 0;
            foreach (var kv in _receiverMap.ToList())
            {
                var item = kv.Value;
                if ((string.IsNullOrEmpty(chId) || item.ChId == chId.Trim()) &&
                    (string.IsNullOrEmpty(msgId) || item.MsgId == msgId.Trim()) &&
                    (string.IsNullOrEmpty(peerId) || item.PeerId == peerId.Trim()))
                {
                    if (_receiverMap.TryRemove(kv.Key, out _))
                    {
                        removedCount++;
                        Log($"[수신 등록 해제(Unregister)] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' 수신 목록에서 제거됨.");
                    }
                }
            }
            return removedCount > 0;
        }

        /// <summary>
        /// 완료되었거나 취소된 Receiver 항목들을 목록에서 정리합니다.
        /// </summary>
        public void RemoveFinishedReceivers()
        {
            foreach (var kv in _receiverMap.ToList())
            {
                kv.Value.RefreshStatus();
                if (kv.Value.Status == TransferStatus.Completed || kv.Value.Status == TransferStatus.Cancelled ||
                    kv.Value.Status == TransferStatus.Expired || kv.Value.Status == TransferStatus.FileMissing)
                {
                    _receiverMap.TryRemove(kv.Key, out _);
                }
            }
        }

        // =========================================================================
        // [기존에 받다가 중지된 파일(.part)이 있는지 및 몇 퍼센트(%) 받았었는지 확인하는 API]
        // =========================================================================

        /// <summary>
        /// [중지된 파일 퍼센트 확인 1 - 로컬 파일명/크기로 즉시 확인]
        /// 다운로드 폴더와 파일명(및 전체 파일 크기)을 지정하면, 기존에 받다가 중지된 파일(.part)이 있는지와
        /// 몇 퍼센트(SavedPercentage) 받았었는지 즉시 확인하여 반환합니다.
        /// </summary>
        public PartialDownloadCheckResult CheckPartialFileProgress(string downloadFolder, string fileName, long totalFileSize, int chunkSize = 64 * 1024)
        {
            var result = new PartialDownloadCheckResult
            {
                FileName = fileName,
                TotalBytes = totalFileSize
            };

            if (string.IsNullOrWhiteSpace(downloadFolder) || string.IsNullOrWhiteSpace(fileName))
            {
                result.SummaryMessage = "다운로드 폴더 또는 파일명이 지정되지 않았습니다.";
                return result;
            }

            try
            {
                if (chunkSize <= 0) chunkSize = 64 * 1024;
                int totalChunks = totalFileSize > 0 ? (int)((totalFileSize + chunkSize - 1) / chunkSize) : 1;
                result.TotalChunks = totalChunks;

                string finalPath = Path.Combine(downloadFolder.Trim(), fileName.Trim());
                string partPath = finalPath + ".part";
                result.FinalFilePath = finalPath;
                result.PartFilePath = partPath;

                // 1) 받다가 중지된 .part 파일이 존재하는 경우
                if (File.Exists(partPath))
                {
                    long partLen = new FileInfo(partPath).Length;
                    int savedChunks = GetSavedChunkIndexFromPartFile(partPath, chunkSize, totalFileSize, totalChunks);
                    long validSavedBytes = totalFileSize > 0 ? Math.Min(totalFileSize, (long)savedChunks * chunkSize) : partLen;
                    double pct = totalFileSize > 0
                        ? Math.Min(100.0, validSavedBytes * 100.0 / totalFileSize)
                        : 0.0;

                    result.HasPartialFile = true;
                    result.SavedBytes = validSavedBytes;
                    result.SavedChunkCount = savedChunks;
                    result.SavedPercentage = pct;
                    result.SummaryMessage = totalFileSize > 0
                        ? $"[중지된 파일 발견] '{fileName}' -> 기존 수신 진행률: {pct:0.0}% ({savedChunks}/{totalChunks} 패킷, {P2PSenderNode.FormatBytes(validSavedBytes)} / {P2PSenderNode.FormatBytes(totalFileSize)}) - 이어받기 가능"
                        : $"[중지된 파일 발견] '{fileName}' -> 기존 수신 용량: {P2PSenderNode.FormatBytes(partLen)} ({savedChunks} 패킷 완료) - 이어받기 가능";

                    Log(result.SummaryMessage);
                    return result;
                }

                // 2) 이미 최종 완료된 파일이 존재하는 경우
                if (File.Exists(finalPath))
                {
                    long finalLen = new FileInfo(finalPath).Length;
                    if (totalFileSize <= 0 || finalLen == totalFileSize)
                    {
                        result.IsAlreadyCompleted = true;
                        result.SavedBytes = finalLen;
                        result.SavedChunkCount = totalChunks;
                        result.SavedPercentage = 100.0;
                        result.SummaryMessage = $"[완료된 파일 존재] '{fileName}' 파일은 이미 100% ({P2PSenderNode.FormatBytes(finalLen)}) 다운로드 완료되어 있습니다.";
                        Log(result.SummaryMessage);
                        return result;
                    }
                }

                // 3) 기존에 받던 파일이 없는 경우
                result.HasPartialFile = false;
                result.SavedPercentage = 0.0;
                result.SummaryMessage = $"[신규 수신 대상] '{fileName}' -> 기존에 받다 중지된 파일(.part)이 없습니다. (0.0% 부터 새로 수신)";
                Log(result.SummaryMessage);
                return result;
            }
            catch (Exception ex)
            {
                result.SummaryMessage = $"중지된 파일 확인 중 오류 발생: {ex.Message}";
                return result;
            }
        }

        /// <summary>
        /// [중지된 파일 퍼센트 확인 2 - 서버에 chID/msgID로 조회하여 로컬 중지 파일 퍼센트 확인]
        /// 수신자가 파일명을 모르고 chID(채널ID)와 msgID(파일ID)만 지정했을 때,
        /// 서버에서 파일 메타데이터(파일명, 전체 크기)를 조회한 뒤 로컬 폴더에 받다 중지된 .part 파일이 몇 퍼센트(%) 받아져 있는지 확인합니다.
        /// </summary>
        public async Task<PartialDownloadCheckResult> CheckPartialProgressFromServerAsync(
            string serverIp, int serverPort, string peerId, string peerName, string chId, string msgId, string downloadFolder)
        {
            var result = new PartialDownloadCheckResult { ChId = chId, MsgId = msgId };

            if (!ValidateConnectionParameters(serverIp, serverPort, peerId, peerName, chId, msgId, downloadFolder, out string validationError))
            {
                result.SummaryMessage = validationError;
                return result;
            }

            // 먼저 현재 메모리 리스트(_receiverMap)에 등록된 항목이 있고 파일명을 이미 알고 있다면 즉시 로컬 확인
            string taskKey = $"{peerId.Trim()}_{chId.Trim()}_{msgId.Trim()}";
            if (_receiverMap.TryGetValue(taskKey, out var existingItem) && !string.IsNullOrEmpty(existingItem.FileName) && existingItem.FileSize > 0)
            {
                var localRes = CheckPartialFileProgress(downloadFolder, existingItem.FileName, existingItem.FileSize);
                localRes.ChId = chId;
                localRes.MsgId = msgId;
                return localRes;
            }

            // 서버에 QueryMetaOnly 명령으로 파일명/전체크기를 물어봐서 로컬 .part 파일의 퍼센트 계산
            using (var client = new TcpClient())
            {
                client.NoDelay = true;
                try
                {
                    await ConnectWithTimeoutAsync(client, serverIp.Trim(), serverPort, ConnectTimeoutMs).ConfigureAwait(false);
                    NetworkStream stream = client.GetStream();

                    var hsReq = new HandshakeRequest
                    {
                        PeerId = peerId.Trim(),
                        PeerName = peerName.Trim(),
                        ChId = chId.Trim(),
                        MsgId = msgId.Trim()
                    };

                    using (var cts = new CancellationTokenSource(ConnectTimeoutMs))
                    {
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.QueryMetaOnly, 0, FrameStreamHelper.SerializeJson(hsReq), cts.Token).ConfigureAwait(false);
                        P2PFrame ackFrame = await FrameStreamHelper.ReadFrameAsync(stream, cts.Token).ConfigureAwait(false);

                        if (ackFrame == null)
                        {
                            result.SummaryMessage = "서버로부터 파일 정보 응답을 받지 못했습니다.";
                            Log("[사전 조회 오류] " + result.SummaryMessage);
                            return result;
                        }

                        if (ackFrame.Command == P2PCommand.Error)
                        {
                            ParseServerErrorAndNotify(ackFrame.GetPayloadText(), chId, msgId, peerId, peerName, out _, out string parsedMsg);
                            result.SummaryMessage = parsedMsg;
                            return result;
                        }

                        var meta = FrameStreamHelper.DeserializeJson<HandshakeResponse>(ackFrame.GetPayloadText());
                        if (meta == null)
                        {
                            result.SummaryMessage = "서버 파일 메타데이터 파싱에 실패했습니다.";
                            return result;
                        }

                        var checkRes = CheckPartialFileProgress(downloadFolder, meta.FileName, meta.FileSize, meta.ChunkSize);
                        checkRes.ChId = chId;
                        checkRes.MsgId = msgId;
                        return checkRes;
                    }
                }
                catch (Exception ex)
                {
                    string errMsg = $"서버({serverIp}:{serverPort})에 접속하여 파일 정보를 확인할 수 없습니다: {ex.Message}";
                    RaiseError(P2PErrorCode.ConnectionFailed, chId, msgId, peerId, peerName, errMsg, ex);
                    result.SummaryMessage = errMsg;
                    return result;
                }
            }
        }

        /// <summary>
        /// 새로운 수신 작업(P2PReceiverItem)을 리스트에 자동 추가하고 백그라운드에서 다운로드를 시작합니다.
        /// 잘못된 입력값(포트, IP, 빈 msgID, 잘못된 폴더 경로 등)이 들어와도 예외로 프로그램이 죽지 않고 상세 오류 메시지를 출력합니다.
        /// </summary>
        public Task<P2PReceiverItem> StartDownloadAsync(string serverIp, int serverPort, string peerId, string peerName, string chId, string msgId, string downloadFolder)
        {
            if (string.IsNullOrWhiteSpace(serverIp)) serverIp = "127.0.0.1";
            if (string.IsNullOrWhiteSpace(peerId)) peerId = "EMP_001";
            if (string.IsNullOrWhiteSpace(peerName)) peerName = "홍길동";
            if (string.IsNullOrWhiteSpace(chId)) chId = "DEFAULT_CH";
            if (string.IsNullOrWhiteSpace(downloadFolder))
                downloadFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Downloads");

            // 입력값 유효성 검증 (잘못된 값 입력 시 예외 대신 오류 메시지 출력 후 안전하게 반환)
            if (!ValidateConnectionParameters(serverIp, serverPort, peerId, peerName, chId, msgId, downloadFolder, out _))
            {
                return Task.FromResult<P2PReceiverItem>(null);
            }

            string normFolder = P2PFileWatchManager.NormalizePath(downloadFolder);
            string taskKey = $"{peerId.Trim()}_{chId.Trim()}_{msgId.Trim()}_{normFolder}";

            // 이미 리스트에 동일한 (peerId, chId, msgId, downloadFolder) 아이템이 현재 수신 진행 중인지 중복 체크
            if (_receiverMap.TryGetValue(taskKey, out var existing))
            {
                existing.RefreshStatus();

                if (existing.IsPaused && existing.Stream != null)
                {
                    existing.IsPaused = false;
                    existing.Status = TransferStatus.Transferring;
                    _ = SendControlAsync(existing, P2PCommand.Resume, existing.CurrentChunkIndex);
                    Log($"[수신 재개] 사번:{peerId}({peerName}), chID:'{chId}', msgID:'{msgId}' 일시정지 해제 -> 이어받기 재개");
                    return Task.FromResult(existing);
                }
                else if (existing.Status == TransferStatus.WaitingQueue || existing.Status == TransferStatus.Transferring || existing.Status == TransferStatus.Merging)
                {
                    Log($"[중복 수신 무시] 이미 동일한 항목(chID:'{chId}', msgID:'{msgId}', 폴더:'{normFolder}')이 현재 수신 진행 중입니다.");
                    return Task.FromResult(existing);
                }
                // 만약 이전에 이미 완료(Completed)된 상태라면, 다시 수신 요청 시 기존 파일 뒤에 (1), (2)를 붙여서 새로 받을 수 있도록 진행 허용
            }

            // 새 Receiver 아이템 생성 후 관리 리스트(_receiverMap)에 자동 등록
            var newItem = new P2PReceiverItem
            {
                TaskKey = taskKey,
                ServerIp = serverIp.Trim(),
                ServerPort = serverPort,
                PeerId = peerId.Trim(),
                PeerName = peerName.Trim(),
                ChId = chId.Trim(),
                MsgId = msgId.Trim(),
                DownloadFolder = normFolder,
                Status = TransferStatus.WaitingQueue,
                StartedTime = DateTime.Now,
                ExpireTimeUtc = DateTime.UtcNow.AddMinutes(30),
                Cts = new CancellationTokenSource()
            };

            _receiverMap[taskKey] = newItem;
            Log($"[수신 목록 등록] 사번:{newItem.PeerId}({newItem.PeerName}), chID:'{newItem.ChId}', msgID:'{newItem.MsgId}' 리스트 추가됨 (총 관리 목록:{_receiverMap.Count}개)");

            // 백그라운드 스레드에서 비동기 다운로드 시작 (UI 블로킹 없음)
            _ = Task.Run(() => RunDownloadSessionAsync(newItem, newItem.DownloadFolder));
            return Task.FromResult(newItem);
        }

        private bool ValidateConnectionParameters(string serverIp, int serverPort, string peerId, string peerName, string chId, string msgId, string downloadFolder, out string errorMsg)
        {
            errorMsg = null;

            if (string.IsNullOrWhiteSpace(msgId))
            {
                errorMsg = "요청할 파일ID(msgID)가 입력되지 않았습니다. 파일ID(msgID)를 입력해주세요.";
                RaiseError(P2PErrorCode.InvalidParameter, chId, msgId, peerId, peerName, errorMsg);
                return false;
            }

            if (serverPort <= 0 || serverPort > 65535)
            {
                errorMsg = $"잘못된 서버 포트 번호({serverPort})입니다. 포트 번호는 1 ~ 65535 사이여야 합니다.";
                RaiseError(P2PErrorCode.InvalidParameter, chId, msgId, peerId, peerName, errorMsg);
                return false;
            }

            string trimmedIp = (serverIp ?? "").Trim();
            if (!trimmedIp.Equals("localhost", StringComparison.OrdinalIgnoreCase) && !IPAddress.TryParse(trimmedIp, out _))
            {
                errorMsg = $"잘못된 서버 IP 주소 형식('{serverIp}')입니다. 올바른 IP 주소(예: 127.0.0.1 또는 192.168.x.x)를 입력해주세요.";
                RaiseError(P2PErrorCode.InvalidParameter, chId, msgId, peerId, peerName, errorMsg);
                return false;
            }

            if (!string.IsNullOrWhiteSpace(downloadFolder) && downloadFolder.IndexOfAny(Path.GetInvalidPathChars()) >= 0)
            {
                errorMsg = $"다운로드 폴더 경로에 허용되지 않는 문자가 포함되어 있습니다: '{downloadFolder}'";
                RaiseError(P2PErrorCode.InvalidParameter, chId, msgId, peerId, peerName, errorMsg);
                return false;
            }

            return true;
        }

        /// <summary>
        /// 리스트에서 특정 채널ID(chId), 파일ID(msgId), 사번(peerId)의 다운로드를 일시정지(Pause)합니다.
        /// </summary>
        public void PauseDownload(string chId = null, string msgId = null, string peerId = null)
        {
            foreach (var item in _receiverMap.Values)
            {
                if ((string.IsNullOrEmpty(chId) || item.ChId == chId) &&
                    (string.IsNullOrEmpty(msgId) || item.MsgId == msgId) &&
                    (string.IsNullOrEmpty(peerId) || item.PeerId == peerId) &&
                    item.Status == TransferStatus.Transferring)
                {
                    item.IsPaused = true;
                    item.Status = TransferStatus.Paused;
                    _ = SendControlAsync(item, P2PCommand.Pause, item.CurrentChunkIndex);
                    Log($"[일시정지] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' (패킷 {item.CurrentChunkIndex}/{item.TotalChunks}, {item.ProgressPercentage:0.0}% 저장됨)");
                    RaiseProgress(item, 64 * 1024, TransferStatus.Paused, "일시정지됨", null);
                }
            }
        }

        /// <summary>
        /// 리스트에서 일시정지된 다운로드를 재개(Resume)합니다.
        /// </summary>
        public void ResumeDownload(string chId = null, string msgId = null, string peerId = null)
        {
            foreach (var item in _receiverMap.Values)
            {
                if ((string.IsNullOrEmpty(chId) || item.ChId == chId) &&
                    (string.IsNullOrEmpty(msgId) || item.MsgId == msgId) &&
                    (string.IsNullOrEmpty(peerId) || item.PeerId == peerId) &&
                    item.IsPaused)
                {
                    item.IsPaused = false;
                    item.Status = TransferStatus.Transferring;
                    _ = SendControlAsync(item, P2PCommand.Resume, item.CurrentChunkIndex);
                    Log($"[재개 요청] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' (패킷 {item.CurrentChunkIndex}번, {item.ProgressPercentage:0.0}%부터 재개)");
                    RaiseProgress(item, 64 * 1024, TransferStatus.Transferring, "전송 재개됨", null);
                }
            }
        }

        /// <summary>
        /// 리스트에서 특정 채널ID(chId), 파일ID(msgId), 사번(peerId)의 다운로드를 취소(Cancel)합니다.
        /// </summary>
        public void CancelDownload(string chId = null, string msgId = null, string peerId = null)
        {
            foreach (var item in _receiverMap.Values)
            {
                if ((string.IsNullOrEmpty(chId) || item.ChId == chId) &&
                    (string.IsNullOrEmpty(msgId) || item.MsgId == msgId) &&
                    (string.IsNullOrEmpty(peerId) || item.PeerId == peerId) &&
                    (item.Status == TransferStatus.WaitingQueue || item.Status == TransferStatus.Transferring || item.Status == TransferStatus.Paused))
                {
                    item.IsCancelled = true;
                    item.Status = TransferStatus.Cancelled;
                    try
                    {
                        SendControlAsync(item, P2PCommand.Cancel, item.CurrentChunkIndex).Wait(100);
                    }
                    catch { }
                    try { item.Cts?.Cancel(); } catch { }
                    Log($"[수신 취소 요청] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' 취소 처리됨.");
                }
            }
        }

        private async Task ConnectWithTimeoutAsync(TcpClient client, string host, int port, int timeoutMs)
        {
            var connectTask = client.ConnectAsync(host, port);
            var delayTask = Task.Delay(timeoutMs);
            var completed = await Task.WhenAny(connectTask, delayTask).ConfigureAwait(false);
            if (completed != connectTask)
            {
                try { client.Close(); } catch { }
                throw new TimeoutException($"서버({host}:{port}) 접속 응답 시간이 초과되었습니다({timeoutMs / 1000.0:0.#}초). 서버가 켜져 있는지 및 IP/포트를 확인해주세요.");
            }
            await connectTask.ConfigureAwait(false); // 연결 중 발생한 SocketException 전파
        }

        private void ParseServerErrorAndNotify(string rawServerError, string chId, string msgId, string peerId, string peerName, out P2PErrorCode errorCode, out string userFriendlyMessage)
        {
            if (string.IsNullOrEmpty(rawServerError))
            {
                errorCode = P2PErrorCode.ProtocolError;
                userFriendlyMessage = "서버에서 알 수 없는 오류를 반환했습니다.";
            }
            else if (rawServerError.StartsWith("UNREGISTERED:"))
            {
                errorCode = P2PErrorCode.UnregisteredFileRequested;
                userFriendlyMessage = rawServerError.Substring("UNREGISTERED:".Length);
            }
            else if (rawServerError.StartsWith("EXPIRED:"))
            {
                errorCode = P2PErrorCode.FileExpired;
                userFriendlyMessage = rawServerError.Substring("EXPIRED:".Length);
            }
            else if (rawServerError.StartsWith("FILE_MISSING:"))
            {
                errorCode = P2PErrorCode.SourceFileNotFound;
                userFriendlyMessage = rawServerError.Substring("FILE_MISSING:".Length);
            }
            else
            {
                errorCode = P2PErrorCode.ProtocolError;
                userFriendlyMessage = rawServerError;
            }

            RaiseError(errorCode, chId, msgId, peerId, peerName, userFriendlyMessage);
        }

        private async Task RunDownloadSessionAsync(P2PReceiverItem item, string downloadFolder)
        {
            // 0. 다운로드 폴더 생성 및 디스크 접근 권한 검증
            try
            {
                if (!Directory.Exists(downloadFolder))
                {
                    Directory.CreateDirectory(downloadFolder);
                }
            }
            catch (Exception dirEx)
            {
                string errMsg = $"다운로드 폴더('{downloadFolder}')를 생성하거나 접근할 수 없습니다: {dirEx.Message}";
                item.Status = TransferStatus.Error;
                item.LastErrorCode = P2PErrorCode.DiskAccessError;
                item.LastErrorMessage = errMsg;
                RaiseError(P2PErrorCode.DiskAccessError, item.ChId, item.MsgId, item.PeerId, item.PeerName, errMsg, dirEx);
                RaiseProgress(item, 64 * 1024, TransferStatus.Error, errMsg, null, P2PErrorCode.DiskAccessError);
                return;
            }

            CancellationToken ct = item.Cts.Token;
            string partFilePath = null;
            int chunkSize = 64 * 1024;

            using (var client = new TcpClient())
            {
                client.NoDelay = true;
                try
                {
                    Log($"[연결 시도] 서버({item.ServerIp}:{item.ServerPort}) 접속 중... (사번:{item.PeerId}, 이름:{item.PeerName}, chID:{item.ChId}, msgID:{item.MsgId})");

                    try
                    {
                        await ConnectWithTimeoutAsync(client, item.ServerIp, item.ServerPort, ConnectTimeoutMs).ConfigureAwait(false);
                    }
                    catch (Exception connEx)
                    {
                        string connErrMsg = $"송신자 서버({item.ServerIp}:{item.ServerPort})에 네트워크 접속을 할 수 없습니다. 서버가 시작(포트 오픈)되었는지, IP 주소와 포트 번호가 맞는지 확인해주세요. ({connEx.Message})";
                        item.Status = TransferStatus.Error;
                        item.LastErrorCode = P2PErrorCode.ConnectionFailed;
                        item.LastErrorMessage = connErrMsg;
                        RaiseError(P2PErrorCode.ConnectionFailed, item.ChId, item.MsgId, item.PeerId, item.PeerName, connErrMsg, connEx);
                        RaiseProgress(item, chunkSize, TransferStatus.Error, connErrMsg, null, P2PErrorCode.ConnectionFailed);
                        return;
                    }

                    NetworkStream stream = client.GetStream();
                    item.Stream = stream;

                    // 1. Handshake 전송: peerID(사번), peerName(이름), chID(채널ID), msgID(파일ID) 전송
                    var hsReq = new HandshakeRequest
                    {
                        PeerId = item.PeerId,
                        PeerName = item.PeerName,
                        ChId = item.ChId,
                        MsgId = item.MsgId
                    };
                    await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Handshake, 0, FrameStreamHelper.SerializeJson(hsReq), ct).ConfigureAwait(false);

                    // 2. 서버로부터 파일 메타데이터(HandshakeAck) 또는 Error(미등록 파일, 유효시간 만료, 원본파일 없음 등) 수신
                    P2PFrame ackFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                    if (ackFrame == null)
                        throw new IOException("서버로부터 초기 응답(HandshakeAck)을 받지 못하고 연결이 종료되었습니다.");

                    if (ackFrame.Command == P2PCommand.Error)
                    {
                        ParseServerErrorAndNotify(ackFrame.GetPayloadText(), item.ChId, item.MsgId, item.PeerId, item.PeerName,
                            out P2PErrorCode errCode, out string friendlyErrMsg);

                        item.LastErrorCode = errCode;
                        item.LastErrorMessage = friendlyErrMsg;
                        item.Status = errCode == P2PErrorCode.FileExpired ? TransferStatus.Expired : TransferStatus.Error;
                        if (errCode == P2PErrorCode.FileExpired) item.ExpireTimeUtc = DateTime.UtcNow.AddSeconds(-1);

                        RaiseProgress(item, chunkSize, item.Status, friendlyErrMsg, null, errCode);
                        return;
                    }

                    if (ackFrame.Command != P2PCommand.HandshakeAck)
                    {
                        string protoErr = $"예상치 못한 서버 프로토콜 응답({ackFrame.Command})입니다.";
                        item.Status = TransferStatus.Error;
                        RaiseError(P2PErrorCode.ProtocolError, item.ChId, item.MsgId, item.PeerId, item.PeerName, protoErr);
                        RaiseProgress(item, chunkSize, TransferStatus.Error, protoErr, null, P2PErrorCode.ProtocolError);
                        return;
                    }

                    var fileMeta = FrameStreamHelper.DeserializeJson<HandshakeResponse>(ackFrame.GetPayloadText());
                    if (fileMeta == null) return;

                    item.FileSize = fileMeta.FileSize;
                    item.TotalChunks = fileMeta.TotalChunks;
                    if (fileMeta.ExpireUtcTicks > 0)
                    {
                        item.ExpireTimeUtc = new DateTime(fileMeta.ExpireUtcTicks, DateTimeKind.Utc);
                    }
                    chunkSize = fileMeta.ChunkSize > 0 ? fileMeta.ChunkSize : 64 * 1024;

                    // 다운로드 폴더에 같은 이름의 파일이 이미 존재하면 "(1)", "(2)"...를 붙인 파일명 결정 (단, 이어받기용 .part가 있으면 해당 .part 유지)
                    string targetFileName = ResolveTargetFileName(downloadFolder, fileMeta.FileName, item.TaskKey);
                    item.FileName = targetFileName;

                    string finalFilePath = Path.Combine(downloadFolder, targetFileName);
                    partFilePath = finalFilePath + ".part";
                    item.PartFilePath = partFilePath;
                    item.SavedFilePath = finalFilePath;
                    item.IsMissingNotified = false;

                    // 기존에 받다가 중지된 파일(.part)이 있는지 및 몇 퍼센트 받았었는지 확인
                    var partialInfo = CheckPartialFileProgress(downloadFolder, targetFileName, item.FileSize, chunkSize);
                    int startChunk = partialInfo.HasPartialFile ? partialInfo.SavedChunkCount : 0;
                    item.CurrentChunkIndex = startChunk;

                    if (partialInfo.HasPartialFile && startChunk > 0)
                    {
                        Log($"[이어받기 감지] '{targetFileName}' 기존에 받다 중지된 파일이 있습니다! (기존 수신률: {partialInfo.SavedPercentage:0.0}%, {startChunk}/{item.TotalChunks} 패킷 완료 -> {startChunk + 1}번째 패킷부터 이어받기)");
                    }
                    else
                    {
                        string renameNote = !string.Equals(targetFileName, fileMeta.FileName, StringComparison.OrdinalIgnoreCase)
                            ? $" (동일 파일 존재로 이름 변경: '{fileMeta.FileName}' -> '{targetFileName}')"
                            : "";
                        Log($"[파일 확인] chID:'{item.ChId}', msgID:'{item.MsgId}' -> 저장파일명:'{targetFileName}'{renameNote}, 크기:{P2PSenderNode.FormatBytes(item.FileSize)}, 총 {item.TotalChunks}패킷 (유효시간:{item.RemainingValidTimeText})");
                    }

                    // 3. 대기열(WaitQueue) 또는 시작 승인(QueueReady) 수신 대기
                    while (true)
                    {
                        P2PFrame qFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                        if (qFrame == null)
                            throw new IOException("대기열 대기 중 송신자 서버와의 연결이 끊어졌습니다.");

                        if (qFrame.Command == P2PCommand.WaitQueue)
                        {
                            item.Status = TransferStatus.WaitingQueue;
                            Log($"[수신 대기중] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' ({item.FileName}) - 서버 동시 전송 제한 대기열 대기 중...");
                            RaiseProgress(item, chunkSize, TransferStatus.WaitingQueue, "동시 전송 제한 대기 중", null);
                            continue;
                        }
                        else if (qFrame.Command == P2PCommand.QueueReady)
                        {
                            item.Status = TransferStatus.Transferring;
                            Log($"[수신 시작] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' ({item.FileName}) 패킷 요청을 시작합니다. (시작 진행률:{item.ProgressPercentage:0.0}%)");
                            break;
                        }
                        else if (qFrame.Command == P2PCommand.Cancel)
                        {
                            item.IsCancelled = true;
                            throw new OperationCanceledException("송신자 서버에 의해 전송이 취소되었습니다.");
                        }
                        else if (qFrame.Command == P2PCommand.Error)
                        {
                            ParseServerErrorAndNotify(qFrame.GetPayloadText(), item.ChId, item.MsgId, item.PeerId, item.PeerName, out P2PErrorCode ec, out string em);
                            item.Status = TransferStatus.Error;
                            RaiseProgress(item, chunkSize, TransferStatus.Error, em, null, ec);
                            return;
                        }
                    }

                    // 4. 한 패킷씩 번호(RequestChunk)를 요청하고 DataChunk를 수신하여 복호화 및 SHA-256 검증
                    using (var partFs = new FileStream(partFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.ReadWrite))
                    {
                        long resumeOffset = Math.Min(item.FileSize, (long)startChunk * chunkSize);
                        partFs.SetLength(resumeOffset);
                        partFs.Seek(resumeOffset, SeekOrigin.Begin);

                        for (int chunkIdx = startChunk; chunkIdx < item.TotalChunks; chunkIdx++)
                        {
                            ct.ThrowIfCancellationRequested();

                            while (item.IsPaused && !ct.IsCancellationRequested && !item.IsCancelled)
                            {
                                await Task.Delay(100, ct).ConfigureAwait(false);
                            }

                            if (item.IsCancelled)
                                throw new OperationCanceledException();

                            item.CurrentChunkIndex = chunkIdx;

                            // 4-1. 송신자에게 chunkIdx번 패킷 요청
                            await SendControlAsync(item, P2PCommand.RequestChunk, chunkIdx).ConfigureAwait(false);

                            // 4-2. 송신자로부터 DataChunk (또는 제어 명령) 수신
                            P2PFrame dataFrame = null;
                            while (true)
                            {
                                dataFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                                if (dataFrame == null)
                                    throw new IOException("패킷 수신 도중 송신자 서버와의 네트워크 연결이 끊어졌습니다.");

                                if (dataFrame.Command == P2PCommand.DataChunk)
                                {
                                    break;
                                }
                                else if (dataFrame.Command == P2PCommand.Cancel)
                                {
                                    item.IsCancelled = true;
                                    throw new OperationCanceledException("송신자가 전송을 취소했습니다.");
                                }
                                else if (dataFrame.Command == P2PCommand.Error)
                                {
                                    throw new IOException("서버 오류 응답: " + dataFrame.GetPayloadText());
                                }
                            }

                            byte[] encrypted = dataFrame.Payload ?? new byte[0];

                            // 복호화 및 SHA-256 무결성 검증
                            byte[] decrypted = ChunkSecurityHelper.TransformChunk(encrypted, encrypted.Length, dataFrame.ChunkIndex);
                            byte[] actualSha256 = ChunkSecurityHelper.ComputeSha256(decrypted, 0, decrypted.Length);
                            if (!ChunkSecurityHelper.VerifyHash(dataFrame.Hash, actualSha256))
                            {
                                string hashErr = $"패킷 #{dataFrame.ChunkIndex} 데이터 무결성(SHA-256) 검증에 실패했습니다! 네트워크 데이터 손상이 의심됩니다.";
                                RaiseError(P2PErrorCode.IntegrityCheckFailed, item.ChId, item.MsgId, item.PeerId, item.PeerName, hashErr);
                                throw new InvalidDataException(hashErr);
                            }

                            if (decrypted.Length > 0)
                            {
                                await partFs.WriteAsync(decrypted, 0, decrypted.Length, ct).ConfigureAwait(false);
                            }
                            await partFs.FlushAsync(ct).ConfigureAwait(false);

                            int doneCount = chunkIdx + 1;
                            item.CurrentChunkIndex = doneCount;
                            item.Status = TransferStatus.Transferring;

                            // 0.5초(500ms, ProgressIntervalMs) 간격 또는 첫 패킷 / 마지막 패킷일 때만 이벤트 및 로그 업데이트
                            long nowTicks = DateTime.UtcNow.Ticks;
                            double elapsedMs = (nowTicks - item.LastProgressTick) / (double)TimeSpan.TicksPerMillisecond;
                            bool shouldNotify = (doneCount == 1) || (doneCount == item.TotalChunks) || (elapsedMs >= ProgressIntervalMs);

                            if (shouldNotify)
                            {
                                item.LastProgressTick = nowTicks;
                                string resumeNote = startChunk > 0 ? $" [이어받기:{startChunk}번~]" : "";
                                Log($"[패킷 수신] <- 사번:{item.PeerId}({item.PeerName}) | chID:{item.ChId} | msgID:{item.MsgId} ({item.FileName}) | 패킷 {doneCount}/{item.TotalChunks} ({item.ProgressPercentage:0.0}%){resumeNote}");
                                RaiseProgress(item, chunkSize, TransferStatus.Transferring, $"수신 중 ({doneCount}/{item.TotalChunks})", null);
                            }
                        }
                    }

                    // 5. 파일 병합(Merge) 완료: ".part" -> 최종 파일명(이미 같은 이름이 생겼다면 (1), (2) 추가 확인)으로 변경
                    item.Status = TransferStatus.Merging;
                    RaiseProgress(item, chunkSize, TransferStatus.Merging, "파일 병합 중...", null);

                    if (File.Exists(finalFilePath))
                    {
                        string uniqueName = GetUniqueAvailableFileName(downloadFolder, fileMeta.FileName);
                        finalFilePath = Path.Combine(downloadFolder, uniqueName);
                        item.FileName = uniqueName;
                    }

                    File.Move(partFilePath, finalFilePath);

                    item.SavedFilePath = finalFilePath;
                    item.Status = TransferStatus.Completed;

                    // 송신자에게 완료(FileCompleted) 통보
                    await SendControlAsync(item, P2PCommand.FileCompleted, item.TotalChunks).ConfigureAwait(false);

                    Log($"[수신 완료] 사번:{item.PeerId}({item.PeerName}) | chID:'{item.ChId}' | msgID:'{item.MsgId}' -> 저장 완료: {finalFilePath}");
                    RaiseProgress(item, chunkSize, TransferStatus.Completed, "수신 완료", finalFilePath);
                }
                catch (OperationCanceledException)
                {
                    if (item.IsCancelled)
                    {
                        item.Status = TransferStatus.Cancelled;
                        CleanupTempPartFile(partFilePath);
                        Log($"[취소 완료] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' 임시 파일(.part) 삭제됨.");
                        RaiseProgress(item, chunkSize, TransferStatus.Cancelled, "취소됨", null);
                    }
                    else
                    {
                        int saved = GetSavedChunkIndexFromPartFile(partFilePath, chunkSize, item.FileSize, item.TotalChunks);
                        item.CurrentChunkIndex = saved;
                        item.Status = TransferStatus.Error;
                        string cancelMsg = $"연결이 중단되었습니다. (현재까지 받은 진행률: {item.ProgressPercentage:0.0}%, {saved}/{item.TotalChunks} 패킷 보관됨 - 다시 수신을 누르면 이어받기됩니다)";
                        RaiseError(P2PErrorCode.NetworkDisconnected, item.ChId, item.MsgId, item.PeerId, item.PeerName, cancelMsg);
                        RaiseProgress(item, chunkSize, TransferStatus.Error, cancelMsg, null, P2PErrorCode.NetworkDisconnected);
                    }
                }
                catch (Exception ex)
                {
                    if (item.IsCancelled)
                    {
                        item.Status = TransferStatus.Cancelled;
                        CleanupTempPartFile(partFilePath);
                        Log($"[취소 완료] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' 임시 파일(.part) 삭제됨.");
                        RaiseProgress(item, chunkSize, TransferStatus.Cancelled, "취소됨", null);
                    }
                    else
                    {
                        int saved = GetSavedChunkIndexFromPartFile(partFilePath, chunkSize, item.FileSize, item.TotalChunks);
                        item.CurrentChunkIndex = saved;
                        item.Status = TransferStatus.Error;
                        P2PErrorCode errCode = ex is InvalidDataException ? P2PErrorCode.IntegrityCheckFailed : P2PErrorCode.NetworkDisconnected;
                        string errMsg = $"수신 중 오류 발생: {ex.Message} (현재까지 받은 진행률: {item.ProgressPercentage:0.0}%, {saved}/{item.TotalChunks} 패킷 보관됨 - 다시 수신을 누르면 이어받기됩니다)";
                        item.LastErrorCode = errCode;
                        item.LastErrorMessage = errMsg;
                        RaiseError(errCode, item.ChId, item.MsgId, item.PeerId, item.PeerName, errMsg, ex);
                        RaiseProgress(item, chunkSize, TransferStatus.Error, errMsg, null, errCode);
                    }
                }
                finally
                {
                    item.Stream = null;
                }
            }
        }

        private async Task SendControlAsync(P2PReceiverItem item, P2PCommand cmd, int chunkIndex)
        {
            if (item.Stream == null) return;
            await item.WriteLock.WaitAsync().ConfigureAwait(false);
            try
            {
                await FrameStreamHelper.WriteControlFrameAsync(item.Stream, cmd, chunkIndex, null, CancellationToken.None).ConfigureAwait(false);
            }
            catch { }
            finally
            {
                item.WriteLock.Release();
            }
        }

        /// <summary>
        /// 다운로드 폴더에 같은 이름의 파일이 이미 있으면 "파일명 (1).확장자", "파일명 (2).확장자"를 붙여서 반환합니다.
        /// 단, 이전에 받다가 중지된 ".part" 파일이 있고 현재 다른 활성 세션이 쓰고 있지 않다면 이어받기를 위해 해당 파일명을 그대로 반환합니다.
        /// </summary>
        private string ResolveTargetFileName(string downloadFolder, string originalFileName, string currentTaskKey)
        {
            lock (_fileNameResolveLock)
            {
                string baseName = Path.GetFileNameWithoutExtension(originalFileName);
                string ext = Path.GetExtension(originalFileName);

                int index = 0;
                while (true)
                {
                    string candidateName = index == 0 ? originalFileName : $"{baseName} ({index}){ext}";
                    string candidateFinalPath = Path.Combine(downloadFolder, candidateName);
                    string candidatePartPath = candidateFinalPath + ".part";

                    // 다른 활성 수신 작업이 현재 이 파일명(또는 .part)을 사용 중인지 확인
                    bool usedByOtherActiveTask = _receiverMap.Values.Any(x =>
                        x.TaskKey != currentTaskKey &&
                        (x.Status == TransferStatus.WaitingQueue || x.Status == TransferStatus.Transferring || x.Status == TransferStatus.Paused || x.Status == TransferStatus.Merging) &&
                        string.Equals(x.SavedFilePath, candidateFinalPath, StringComparison.OrdinalIgnoreCase));

                    if (usedByOtherActiveTask)
                    {
                        index++;
                        continue;
                    }

                    // 1) 이전에 받다가 중지된 .part 파일이 존재하면 이어받기를 위해 이 이름을 선택
                    if (File.Exists(candidatePartPath))
                    {
                        return candidateName;
                    }

                    // 2) 완성된 파일이 이미 존재하면 덮어쓰지 않고 (1), (2)... 다음 번호로 넘어감
                    if (File.Exists(candidateFinalPath))
                    {
                        index++;
                        continue;
                    }

                    // 3) 사용 가능하고 충돌 없는 파일명 확정
                    return candidateName;
                }
            }
        }

        private string GetUniqueAvailableFileName(string downloadFolder, string originalFileName)
        {
            lock (_fileNameResolveLock)
            {
                string baseName = Path.GetFileNameWithoutExtension(originalFileName);
                string ext = Path.GetExtension(originalFileName);

                int index = 0;
                while (true)
                {
                    string candidateName = index == 0 ? originalFileName : $"{baseName} ({index}){ext}";
                    string candidateFinalPath = Path.Combine(downloadFolder, candidateName);
                    if (!File.Exists(candidateFinalPath))
                    {
                        return candidateName;
                    }
                    index++;
                }
            }
        }

        /// <summary>
        /// 별도의 .idx 파일 없이, "원래파일명.part"의 현재 파일 크기를 패킷 크기(64KB)로 나누어
        /// 기존에 몇 번 패킷까지 정상 수신되었는지 즉시 계산합니다.
        /// </summary>
        private int GetSavedChunkIndexFromPartFile(string partPath, int chunkSize, long totalFileSize, int totalChunks)
        {
            try
            {
                if (string.IsNullOrEmpty(partPath) || !File.Exists(partPath) || chunkSize <= 0)
                    return 0;

                long len = new FileInfo(partPath).Length;
                if (len <= 0) return 0;

                if (totalFileSize > 0 && len >= totalFileSize && totalChunks > 0)
                {
                    return totalChunks;
                }

                int completedChunks = (int)(len / chunkSize);
                if (totalChunks > 0 && completedChunks > totalChunks)
                {
                    return 0;
                }
                return Math.Max(0, completedChunks);
            }
            catch
            {
                return 0;
            }
        }

        private void CleanupTempPartFile(string partPath)
        {
            try { if (!string.IsNullOrEmpty(partPath) && File.Exists(partPath)) File.Delete(partPath); } catch { }
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

        private void RaiseProgress(P2PReceiverItem item, int chunkSize, TransferStatus status, string message, string savedPath, P2PErrorCode errorCode = P2PErrorCode.None)
        {
            long transferred = Math.Min(item.FileSize, (long)item.CurrentChunkIndex * chunkSize);
            OnProgress?.Invoke(this, new TransferProgressEventArgs
            {
                SessionKey = item.TaskKey,
                ChId = item.ChId,
                MsgId = item.MsgId,
                FileName = item.FileName ?? item.MsgId,
                PeerId = item.PeerId,
                PeerName = item.PeerName,
                CurrentChunkIndex = item.CurrentChunkIndex,
                TotalChunks = item.TotalChunks,
                ChunkSize = chunkSize,
                TransferredBytes = transferred,
                TotalBytes = item.FileSize,
                Status = status,
                ErrorCode = errorCode,
                ExpireTimeUtc = item.ExpireTimeUtc,
                Message = message,
                SavedFilePath = savedPath
            });
        }

        public void Dispose()
        {
            foreach (var kv in _receiverMap)
            {
                try { kv.Value.Cts?.Cancel(); } catch { }
            }
            _receiverMap.Clear();
        }
    }
}
