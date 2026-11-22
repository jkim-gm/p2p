using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace P2PTransferLib
{
    /// <summary>
    /// 개별 수신 작업(Receiver 인스턴스 1개)의 상태, 유효시간 및 제어 정보를 담는 클래스
    /// P2PReceiverNode.Instance.GetReceiverList()로 조회할 때 상태와 남은 유효시간, 파일 존재 여부('파일없음')까지 모두 확인할 수 있습니다.
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
        public long FileSize { get; set; }
        public int CurrentChunkIndex { get; set; }
        public int TotalChunks { get; set; }
        public double ProgressPercentage => TotalChunks > 0 ? Math.Min(100.0, CurrentChunkIndex * 100.0 / TotalChunks) : 0.0;
        public TransferStatus Status { get; set; }
        public string SavedFilePath { get; set; }
        public DateTime StartedTime { get; set; }
        public DateTime ExpireTimeUtc { get; set; }

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
            // 1. 완료된 파일이 로컬 디스크에서 사라졌으면 '파일없음(FileMissing)' 상태로 변경
            if (Status == TransferStatus.Completed && !string.IsNullOrEmpty(SavedFilePath))
            {
                if (!File.Exists(SavedFilePath))
                {
                    Status = TransferStatus.FileMissing;
                }
            }

            // 2. 완료/취소되지 않은 상태에서 유효시간(30분)이 지났으면 'Expired' 표시
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
        internal bool IsPaused { get; set; }
        internal bool IsCancelled { get; set; }
    }

    /// <summary>
    /// P2P 수신자(클라이언트) 자동 리스트 관리 모듈
    /// - StartDownloadAsync()를 호출할 때마다 P2PReceiverItem 객체가 생성되어 내부 리스트(_receiverMap)에 자동 등록됩니다.
    /// - GetReceiverList()로 수신 목록을 조회하면 각 파일의 상태(Status), 진행률(%), 남은 유효시간(RemainingValidTimeText), 완료 파일 삭제 여부(FileMissing)를 즉시 확인할 수 있습니다.
    /// </summary>
    public class P2PReceiverNode : IDisposable
    {
        /// <summary>
        /// 어디서든 접근 가능한 글로벌 수신 매니저 싱글톤 인스턴스
        /// </summary>
        public static P2PReceiverNode Instance { get; } = new P2PReceiverNode();

        // 여러 개의 수신 작업(Receiver)을 자동으로 관리하는 스레드 안전 컬렉션 (Key: "{peerId}_{chId}_{msgId}")
        private readonly ConcurrentDictionary<string, P2PReceiverItem> _receiverMap = new ConcurrentDictionary<string, P2PReceiverItem>();

        /// <summary>
        /// [수신 파일 목록 조회 1]
        /// 현재 등록되어 관리 중인 모든 Receiver 목록을 리스트로 반환합니다.
        /// 조회 시 자동으로 완료된 파일의 디스크 존재 여부('파일없음')와 유효시간(30분) 만료 상태를 최신으로 갱신하여 반환합니다.
        /// (chId를 지정하면 해당 채널의 수신 목록만 필터링 가능)
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

        /// <summary>
        /// 단순 로그 출력 이벤트
        /// </summary>
        public event Action<string> OnLog;

        /// <summary>
        /// 진행 상태 및 완료/취소/오류 이벤트 (WaitingQueue, Transferring, Paused, Merging, Completed, Cancelled, Expired, Error, FileMissing)
        /// </summary>
        public event EventHandler<TransferProgressEventArgs> OnProgress;

        /// <summary>
        /// 새로운 수신 작업(P2PReceiverItem)을 리스트에 자동 추가하고 백그라운드에서 다운로드를 시작합니다.
        /// (peerID:사번, peerName:이름, chID:채널ID, msgID:파일ID 전송)
        /// </summary>
        public Task<P2PReceiverItem> StartDownloadAsync(string serverIp, int serverPort, string peerId, string peerName, string chId, string msgId, string downloadFolder)
        {
            if (string.IsNullOrWhiteSpace(serverIp)) serverIp = "127.0.0.1";
            if (string.IsNullOrWhiteSpace(peerId)) peerId = "EMP_001";
            if (string.IsNullOrWhiteSpace(peerName)) peerName = "홍길동";
            if (string.IsNullOrWhiteSpace(chId)) chId = "DEFAULT_CH";
            if (string.IsNullOrWhiteSpace(msgId))
                throw new ArgumentException("요청할 파일ID(msgID)를 입력해야 합니다.");

            if (string.IsNullOrWhiteSpace(downloadFolder))
                downloadFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Downloads");

            string taskKey = $"{peerId.Trim()}_{chId.Trim()}_{msgId.Trim()}";

            // 이미 리스트에 존재하고 현재 활성 상태인 경우 확인
            if (_receiverMap.TryGetValue(taskKey, out var existing))
            {
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
                    Log($"[알림] 사번:{peerId}({peerName}), chID:'{chId}', msgID:'{msgId}' 는 이미 수신 목록에서 진행 중입니다. (현재 활성 수신 수: {GetActiveReceiverList().Count}개)");
                    return Task.FromResult(existing);
                }
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
                Status = TransferStatus.WaitingQueue,
                StartedTime = DateTime.Now,
                ExpireTimeUtc = DateTime.UtcNow.AddMinutes(30), // 서버 응답 시 서버의 실제 ExpireTimeUtc로 업데이트됨
                Cts = new CancellationTokenSource()
            };

            _receiverMap[taskKey] = newItem;
            Log($"[수신 목록 등록] 사번:{newItem.PeerId}({newItem.PeerName}), chID:'{newItem.ChId}', msgID:'{newItem.MsgId}' 리스트 추가됨 (총 관리 목록:{_receiverMap.Count}개)");

            // 백그라운드 스레드에서 비동기 다운로드 시작 (UI 블로킹 없음)
            _ = Task.Run(() => RunDownloadSessionAsync(newItem, downloadFolder));
            return Task.FromResult(newItem);
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
                    Log($"[일시정지] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' (패킷 {item.CurrentChunkIndex}/{item.TotalChunks} 저장됨)");
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
                    Log($"[재개 요청] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' (패킷 {item.CurrentChunkIndex}번부터 재개)");
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

        private async Task RunDownloadSessionAsync(P2PReceiverItem item, string downloadFolder)
        {
            if (!Directory.Exists(downloadFolder))
            {
                Directory.CreateDirectory(downloadFolder);
            }

            CancellationToken ct = item.Cts.Token;
            string partFilePath = Path.Combine(downloadFolder, $".p2p_{item.PeerId}_{item.ChId}_{item.MsgId}.part");
            string idxFilePath = Path.Combine(downloadFolder, $".p2p_{item.PeerId}_{item.ChId}_{item.MsgId}.idx");
            int chunkSize = 64 * 1024;

            using (var client = new TcpClient())
            {
                client.NoDelay = true;
                try
                {
                    // 기존에 받은 패킷 번호 확인 (이어받기)
                    int startChunk = ReadSavedChunkIndex(idxFilePath);

                    Log($"[연결 시도] 서버({item.ServerIp}:{item.ServerPort}) 접속 중... (사번:{item.PeerId}, 이름:{item.PeerName}, chID:{item.ChId}, msgID:{item.MsgId})");
                    await client.ConnectAsync(item.ServerIp, item.ServerPort).ConfigureAwait(false);
                    NetworkStream stream = client.GetStream();
                    item.Stream = stream;

                    // 1. Handshake 전송: peerID(사번), peerName(이름), chID(채널ID), msgID(파일ID) 및 시작 패킷 번호 전송
                    var hsReq = new HandshakeRequest
                    {
                        PeerId = item.PeerId,
                        PeerName = item.PeerName,
                        ChId = item.ChId,
                        MsgId = item.MsgId
                    };
                    await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Handshake, startChunk, FrameStreamHelper.SerializeJson(hsReq), ct).ConfigureAwait(false);

                    // 2. 서버로부터 파일 메타데이터(HandshakeAck) 또는 Error 수신
                    P2PFrame ackFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                    if (ackFrame == null)
                        throw new IOException("서버로부터 응답이 없습니다.");

                    if (ackFrame.Command == P2PCommand.Error)
                    {
                        string errMsg = ackFrame.GetPayloadText();
                        bool isExpired = errMsg.StartsWith("EXPIRED:");
                        item.Status = isExpired ? TransferStatus.Expired : TransferStatus.Error;
                        if (isExpired) item.ExpireTimeUtc = DateTime.UtcNow.AddSeconds(-1);
                        Log($"[서버 거부] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' -> {errMsg}");
                        RaiseProgress(item, chunkSize, item.Status, errMsg, null);
                        return;
                    }

                    if (ackFrame.Command != P2PCommand.HandshakeAck)
                    {
                        item.Status = TransferStatus.Error;
                        Log($"[프로토콜 오류] 예상치 못한 응답({ackFrame.Command})");
                        RaiseProgress(item, chunkSize, TransferStatus.Error, "프로토콜 응답 오류", null);
                        return;
                    }

                    var fileMeta = FrameStreamHelper.DeserializeJson<HandshakeResponse>(ackFrame.GetPayloadText());
                    if (fileMeta == null) return;

                    item.FileName = fileMeta.FileName;
                    item.FileSize = fileMeta.FileSize;
                    item.TotalChunks = fileMeta.TotalChunks;
                    if (fileMeta.ExpireUtcTicks > 0)
                    {
                        item.ExpireTimeUtc = new DateTime(fileMeta.ExpireUtcTicks, DateTimeKind.Utc);
                    }
                    chunkSize = fileMeta.ChunkSize > 0 ? fileMeta.ChunkSize : 64 * 1024;

                    // 임시 파일(.part) 유효성 확인 (마지막 패킷이 64KB 미만일 수 있으므로 Math.Min으로 실제 기대 바이트 계산)
                    if (startChunk > fileMeta.TotalChunks)
                    {
                        startChunk = 0;
                        SaveChunkIndex(idxFilePath, 0);
                    }
                    long expectedPartSize = Math.Min(fileMeta.FileSize, (long)startChunk * chunkSize);
                    if (startChunk > 0 && (!File.Exists(partFilePath) || new FileInfo(partFilePath).Length < expectedPartSize))
                    {
                        startChunk = 0;
                        SaveChunkIndex(idxFilePath, 0);
                    }
                    item.CurrentChunkIndex = startChunk;

                    Log($"[파일 확인] chID:'{item.ChId}', msgID:'{item.MsgId}' -> 파일명:'{item.FileName}', 크기:{P2PSenderNode.FormatBytes(item.FileSize)}, 총 {item.TotalChunks}패킷 (유효시간:{item.RemainingValidTimeText}, 시작패킷:{startChunk}번)");

                    // 3. 대기열(WaitQueue) 또는 시작 승인(QueueReady) 수신 대기
                    while (true)
                    {
                        P2PFrame qFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                        if (qFrame == null)
                            throw new IOException("대기 중 서버 연결이 끊어졌습니다.");

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
                            Log($"[수신 시작] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' ({item.FileName}) 패킷 요청을 시작합니다.");
                            break;
                        }
                        else if (qFrame.Command == P2PCommand.Cancel)
                        {
                            item.IsCancelled = true;
                            throw new OperationCanceledException("서버에 의해 취소됨");
                        }
                        else if (qFrame.Command == P2PCommand.Error)
                        {
                            string err = qFrame.GetPayloadText();
                            item.Status = TransferStatus.Error;
                            Log($"[오류] {err}");
                            RaiseProgress(item, chunkSize, TransferStatus.Error, err, null);
                            return;
                        }
                    }

                    // 4. 한 패킷씩 번호(RequestChunk)를 요청하고 DataChunk를 수신하여 복호화 및 SHA-256 검증
                    using (var partFs = new FileStream(partFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                    {
                        partFs.Seek((long)startChunk * chunkSize, SeekOrigin.Begin);

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
                                    throw new IOException("패킷 수신 중 서버 연결이 끊어졌습니다.");

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
                                    throw new IOException("서버 오류: " + dataFrame.GetPayloadText());
                                }
                            }

                            byte[] encrypted = dataFrame.Payload ?? new byte[0];

                            // 복호화 및 SHA-256 무결성 검증
                            byte[] decrypted = ChunkSecurityHelper.TransformChunk(encrypted, encrypted.Length, dataFrame.ChunkIndex);
                            byte[] actualSha256 = ChunkSecurityHelper.ComputeSha256(decrypted, 0, decrypted.Length);
                            if (!ChunkSecurityHelper.VerifyHash(dataFrame.Hash, actualSha256))
                            {
                                throw new InvalidDataException($"패킷 #{dataFrame.ChunkIndex} SHA-256 무결성 검증 실패!");
                            }

                            if (decrypted.Length > 0)
                            {
                                await partFs.WriteAsync(decrypted, 0, decrypted.Length, ct).ConfigureAwait(false);
                            }
                            await partFs.FlushAsync(ct).ConfigureAwait(false);

                            int doneCount = chunkIdx + 1;
                            item.CurrentChunkIndex = doneCount;
                            item.Status = TransferStatus.Transferring;
                            SaveChunkIndex(idxFilePath, doneCount);

                            if (doneCount == 1 || doneCount == item.TotalChunks || doneCount % 5 == 0)
                            {
                                string resumeNote = startChunk > 0 ? $" [이어받기:{startChunk}번~]" : "";
                                Log($"[패킷 수신] <- 사번:{item.PeerId}({item.PeerName}) | chID:{item.ChId} | msgID:{item.MsgId} ({item.FileName}) | 패킷 {doneCount}/{item.TotalChunks} ({item.ProgressPercentage:0.0}%){resumeNote}");
                            }

                            RaiseProgress(item, chunkSize, TransferStatus.Transferring, $"수신 중 ({doneCount}/{item.TotalChunks})", null);
                        }
                    }

                    // 5. 파일 병합(Merge) 및 완료 상태 알림
                    item.Status = TransferStatus.Merging;
                    RaiseProgress(item, chunkSize, TransferStatus.Merging, "파일 병합 중...", null);

                    string finalFileName = $"{item.PeerId}_{item.ChId}_{item.MsgId}_{item.FileName}";
                    string finalFilePath = Path.Combine(downloadFolder, finalFileName);
                    if (File.Exists(finalFilePath))
                    {
                        try { File.Delete(finalFilePath); } catch { }
                    }

                    File.Move(partFilePath, finalFilePath);
                    if (File.Exists(idxFilePath))
                    {
                        try { File.Delete(idxFilePath); } catch { }
                    }

                    item.SavedFilePath = finalFilePath;
                    item.Status = TransferStatus.Completed;

                    // 송신자에게 완료(FileCompleted) 통보
                    await SendControlAsync(item, P2PCommand.FileCompleted, item.TotalChunks).ConfigureAwait(false);

                    Log($"[수신 완료] 사번:{item.PeerId}({item.PeerName}) | chID:'{item.ChId}' | msgID:'{item.MsgId}' 병합 완료 -> 저장경로: {finalFilePath}");
                    RaiseProgress(item, chunkSize, TransferStatus.Completed, "수신 완료", finalFilePath);
                }
                catch (OperationCanceledException)
                {
                    if (item.IsCancelled)
                    {
                        item.Status = TransferStatus.Cancelled;
                        CleanupTempFiles(partFilePath, idxFilePath);
                        Log($"[취소 완료] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' 임시 조각 파일 삭제됨.");
                        RaiseProgress(item, chunkSize, TransferStatus.Cancelled, "취소됨", null);
                    }
                    else
                    {
                        int saved = ReadSavedChunkIndex(idxFilePath);
                        item.CurrentChunkIndex = saved;
                        item.Status = TransferStatus.Error;
                        Log($"[연결 중단] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' ({saved}번 패킷까지 보관됨 - 다시 수신 누르면 이어받기)");
                        RaiseProgress(item, chunkSize, TransferStatus.Error, "연결 중단 (이어받기 대기)", null);
                    }
                }
                catch (Exception ex)
                {
                    if (item.IsCancelled)
                    {
                        item.Status = TransferStatus.Cancelled;
                        CleanupTempFiles(partFilePath, idxFilePath);
                        Log($"[취소 완료] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' 임시 조각 파일 삭제됨.");
                        RaiseProgress(item, chunkSize, TransferStatus.Cancelled, "취소됨", null);
                    }
                    else
                    {
                        int saved = ReadSavedChunkIndex(idxFilePath);
                        item.CurrentChunkIndex = saved;
                        item.Status = TransferStatus.Error;
                        Log($"[수신 오류] 사번:{item.PeerId}({item.PeerName}), chID:'{item.ChId}', msgID:'{item.MsgId}' - {ex.Message} ({saved}번 패킷까지 보관됨 - 다시 수신 누르면 이어받기)");
                        RaiseProgress(item, chunkSize, TransferStatus.Error, ex.Message, null);
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

        private int ReadSavedChunkIndex(string idxPath)
        {
            try
            {
                if (File.Exists(idxPath))
                {
                    string txt = File.ReadAllText(idxPath).Trim();
                    if (int.TryParse(txt, out int idx) && idx >= 0) return idx;
                }
            }
            catch { }
            return 0;
        }

        private void SaveChunkIndex(string idxPath, int completedChunks)
        {
            try { File.WriteAllText(idxPath, completedChunks.ToString()); } catch { }
        }

        private void CleanupTempFiles(string partPath, string idxPath)
        {
            try { if (File.Exists(partPath)) File.Delete(partPath); } catch { }
            try { if (File.Exists(idxPath)) File.Delete(idxPath); } catch { }
        }

        private void Log(string msg)
        {
            OnLog?.Invoke($"[{DateTime.Now:HH:mm:ss.fff}] {msg}");
        }

        private void RaiseProgress(P2PReceiverItem item, int chunkSize, TransferStatus status, string message, string savedPath)
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
