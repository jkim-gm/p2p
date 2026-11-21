using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace P2PTransferLib
{
    /// <summary>
    /// P2P 수신자(B, C, D, E, F...) 측 순수 P2P 파일 수신 엔진 클래스 (채팅 통신 분리됨)
    /// - 채팅(외부 앱)으로부터 최초 전달받은 TransferOffer(파일명/크기/유효시간)를 등록
    /// - 수신 수락 시 P2P 포트로 직접 연결하여 Handshake 및 조각 번호(RequestChunk) 요청
    /// - 패킷 크기가 일정하므로 조각 번호(ChunkIndex)만으로 진행률 산출
    /// - 프레임 헤더의 P2PCommand로 데이터 패킷(DataChunk)과 제어 명령(WaitQueue, QueueReady, Pause, Resume, Cancel, Disconnect 등) 처리
    /// - 다중 파일 순차 수신, 조각 복호화 및 SHA-256 무결성 검증 후 병합(Merge)
    /// - 일시정지(Pause) / 재개(Resume) / 취소(Cancel) / 네트워크 단절 시 재접속 이어받기 지원
    /// </summary>
    public class P2PReceiverNode : IDisposable
    {
        public string PeerId { get; }
        public string PeerName { get; set; }

        private string _downloadFolder;
        /// <summary>
        /// 다운로드 받을 폴더 경로
        /// </summary>
        public string DownloadFolder
        {
            get => _downloadFolder;
            set
            {
                _downloadFolder = value;
                if (!string.IsNullOrWhiteSpace(_downloadFolder) && !Directory.Exists(_downloadFolder))
                {
                    try { Directory.CreateDirectory(_downloadFolder); } catch { }
                }
            }
        }

        // 등록된 파일 전송 공지 목록 (Key: TransferId)
        private readonly ConcurrentDictionary<string, TransferOffer> _receivedOffers = new ConcurrentDictionary<string, TransferOffer>();

        // 사용자가 수락한 TransferId 목록 (재접속 시 이어받기 판단용)
        private readonly ConcurrentDictionary<string, bool> _acceptedOffers = new ConcurrentDictionary<string, bool>();

        // 취소/일시정지 상태 추적 (Key: TransferId)
        private readonly ConcurrentDictionary<string, bool> _cancelledOffers = new ConcurrentDictionary<string, bool>();
        private readonly ConcurrentDictionary<string, bool> _pausedOffers = new ConcurrentDictionary<string, bool>();

        // 완료된 파일 경로 추적 (Key: FileId, Value: SavedFilePath)
        private readonly ConcurrentDictionary<string, string> _completedFiles = new ConcurrentDictionary<string, string>();

        // 현재 실행 중인 P2P 다운로드 연결 컨텍스트 (Key: TransferId)
        private readonly ConcurrentDictionary<string, ActiveDownloadContext> _activeDownloads = new ConcurrentDictionary<string, ActiveDownloadContext>();

        // 다중 파일 순차 다운로드 보장을 위한 세마포어
        private readonly SemaphoreSlim _sequentialDownloadSemaphore = new SemaphoreSlim(1, 1);

        public event EventHandler<TransferProgressEventArgs> TransferProgressUpdated;

        private class ActiveDownloadContext
        {
            public string TransferId { get; set; }
            public string CurrentFileId { get; set; }
            public int CurrentChunkIndex { get; set; }
            public NetworkStream Stream { get; set; }
            public SemaphoreSlim WriteLock { get; } = new SemaphoreSlim(1, 1);
            public CancellationTokenSource Cts { get; set; }
        }

        public P2PReceiverNode(string peerId, string peerName, string defaultDownloadFolder = null)
        {
            PeerId = string.IsNullOrWhiteSpace(peerId) ? Guid.NewGuid().ToString("N") : peerId;
            PeerName = string.IsNullOrWhiteSpace(peerName) ? PeerId : peerName;
            DownloadFolder = string.IsNullOrWhiteSpace(defaultDownloadFolder)
                ? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Downloads", PeerName)
                : defaultDownloadFolder;
        }

        /// <summary>
        /// 채팅에서 최초 "파일 보낼 것이 있다(파일명/크기)"고 받았을 때 P2P 엔진에 해당 메타데이터를 등록합니다.
        /// 등록 즉시 30분 유효시간 카운트가 관리되며 초기 상태(PendingAccept) 이벤트가 발생합니다.
        /// </summary>
        public void RegisterTransferOffer(TransferOffer offer)
        {
            if (offer == null || string.IsNullOrEmpty(offer.TransferId)) return;

            int defaultChunkSize = offer.ChunkSize > 0 ? offer.ChunkSize : 64 * 1024;
            foreach (var f in offer.Files)
            {
                if (f.ChunkSize <= 0) f.ChunkSize = defaultChunkSize;
                if (f.TotalChunks <= 0)
                {
                    int tc = (int)((f.FileSize + f.ChunkSize - 1) / f.ChunkSize);
                    f.TotalChunks = tc == 0 ? 1 : tc;
                }
            }

            _receivedOffers[offer.TransferId] = offer;

            foreach (var f in offer.Files)
            {
                int savedChunk = ReadSavedChunkProgress(offer.TransferId, f.FileId);
                RaiseProgress(offer.TransferId, f.FileId, f.FileName, savedChunk, f.TotalChunks, f.ChunkSize, f.FileSize,
                    TransferStatus.PendingAccept, "수신 수락 대기 중 (30분 유효)", null);
            }
        }

        /// <summary>
        /// 수신자가 P2P 연결(수신 시작)을 누르기 전에 송신자가 채팅 메시지로 송신 취소를 알려왔을 때 호출합니다.
        /// </summary>
        public void NotifyCancelledBySenderBeforeConnection(string transferId)
        {
            HandleCancelledBySender(transferId);
        }

        /// <summary>
        /// 수신자가 수신 수락(또는 이어받기)을 실행합니다.
        /// 30분 유효시간 이내인지 확인한 후, 송신자의 P2P 포트로 직접 접속하여 조각 번호(RequestChunk) 요청을 시작합니다.
        /// </summary>
        public Task<bool> StartOrResumeTransferAsync(string transferId, string senderIp = null, int p2pPort = 0)
        {
            if (!_receivedOffers.TryGetValue(transferId, out var offer))
                return Task.FromResult(false);

            if (!string.IsNullOrWhiteSpace(senderIp))
                offer.SenderIp = senderIp;
            if (p2pPort > 0)
                offer.P2PPort = p2pPort;

            // 30분 유효시간 체크
            if (offer.IsExpired)
            {
                foreach (var f in offer.Files)
                {
                    RaiseProgress(transferId, f.FileId, f.FileName, 0, f.TotalChunks, f.ChunkSize, f.FileSize,
                        TransferStatus.Expired, "유효시간(30분) 경과로 전송 무효화됨", null);
                }
                return Task.FromResult(false);
            }

            _cancelledOffers.TryRemove(transferId, out _);

            // 만약 현재 일시정지(Paused) 중이고 소켓 세션이 살아있다면 즉시 Resume 제어 명령을 보냄
            if (_pausedOffers.TryRemove(transferId, out _) && _activeDownloads.TryGetValue(transferId, out var activeCtx) && activeCtx.Stream != null)
            {
                _ = Task.Run(async () =>
                {
                    try
                    {
                        await activeCtx.WriteLock.WaitAsync().ConfigureAwait(false);
                        try
                        {
                            await FrameStreamHelper.WriteControlFrameAsync(activeCtx.Stream, P2PCommand.Resume, activeCtx.CurrentChunkIndex, null, CancellationToken.None).ConfigureAwait(false);
                        }
                        finally
                        {
                            activeCtx.WriteLock.Release();
                        }
                    }
                    catch { }
                });
                return Task.FromResult(true);
            }

            _acceptedOffers[transferId] = true;

            // 이미 다운로드 작업이 돌고 있지 않다면 P2P 순차 다운로드 태스크 시작
            if (!_activeDownloads.ContainsKey(transferId))
            {
                _ = Task.Run(() => RunSequentialDownloadAsync(offer));
            }

            return Task.FromResult(true);
        }

        /// <summary>
        /// 진행 중인 P2P 전송을 일시정지(Pause 제어 명령 전송)합니다.
        /// </summary>
        public async Task PauseTransferAsync(string transferId)
        {
            _pausedOffers[transferId] = true;

            if (_activeDownloads.TryGetValue(transferId, out var ctx) && ctx.Stream != null)
            {
                try
                {
                    await ctx.WriteLock.WaitAsync().ConfigureAwait(false);
                    try
                    {
                        await FrameStreamHelper.WriteControlFrameAsync(ctx.Stream, P2PCommand.Pause, ctx.CurrentChunkIndex, null, CancellationToken.None).ConfigureAwait(false);
                    }
                    finally
                    {
                        ctx.WriteLock.Release();
                    }
                }
                catch { }

                if (_receivedOffers.TryGetValue(transferId, out var offer))
                {
                    var f = offer.Files.FirstOrDefault(x => x.FileId == ctx.CurrentFileId) ?? offer.Files.FirstOrDefault();
                    if (f != null)
                    {
                        RaiseProgress(transferId, f.FileId, f.FileName, ctx.CurrentChunkIndex, f.TotalChunks, f.ChunkSize, f.FileSize,
                            TransferStatus.Paused, $"일시정지됨 ({ctx.CurrentChunkIndex}/{f.TotalChunks} 패킷 보관됨 - 이어받기 가능)", null);
                    }
                }
            }
        }

        /// <summary>
        /// 수신자가 P2P 제어 명령(Cancel)을 통해 전송을 취소하고 임시 조각 파일을 삭제합니다.
        /// </summary>
        public async Task CancelTransferAsync(string transferId)
        {
            _cancelledOffers[transferId] = true;
            _acceptedOffers.TryRemove(transferId, out _);
            _pausedOffers.TryRemove(transferId, out _);

            if (_activeDownloads.TryGetValue(transferId, out var ctx))
            {
                if (ctx.Stream != null)
                {
                    try
                    {
                        await ctx.WriteLock.WaitAsync().ConfigureAwait(false);
                        try
                        {
                            await FrameStreamHelper.WriteControlFrameAsync(ctx.Stream, P2PCommand.Cancel, ctx.CurrentChunkIndex, null, CancellationToken.None).ConfigureAwait(false);
                        }
                        finally
                        {
                            ctx.WriteLock.Release();
                        }
                    }
                    catch { }
                }
                try { ctx.Cts?.Cancel(); } catch { }
            }

            if (_receivedOffers.TryGetValue(transferId, out var offer))
            {
                foreach (var f in offer.Files)
                {
                    CleanupTempFiles(GetPartFilePath(transferId, f.FileId), GetIdxFilePath(transferId, f.FileId));
                    RaiseProgress(transferId, f.FileId, f.FileName, 0, f.TotalChunks, f.ChunkSize, f.FileSize,
                        TransferStatus.Cancelled, "수신 취소됨", null);
                }
            }
        }

        /// <summary>
        /// 송신자 재접속 시 미완료(오류/네트워크 끊김) 상태였던 전송 건들을 자동으로 이어받기합니다.
        /// </summary>
        public async Task AutoResumeIncompleteTransfersAsync(string senderIp = null, int p2pPort = 0)
        {
            foreach (var kv in _acceptedOffers)
            {
                string transferId = kv.Key;
                if (_cancelledOffers.ContainsKey(transferId) || _pausedOffers.ContainsKey(transferId)) continue;
                if (_activeDownloads.ContainsKey(transferId)) continue;

                if (_receivedOffers.TryGetValue(transferId, out var offer) && !offer.IsExpired)
                {
                    bool hasIncomplete = offer.Files.Any(f => !_completedFiles.ContainsKey(f.FileId));
                    if (hasIncomplete)
                    {
                        await StartOrResumeTransferAsync(transferId, senderIp, p2pPort).ConfigureAwait(false);
                    }
                }
            }
        }

        /// <summary>
        /// 외부에서 송신자 네트워크 끊김을 감지했을 때 호출하거나, P2P 스트림 단절 시 상태를 갱신합니다.
        /// </summary>
        public void NotifySenderOffline()
        {
            foreach (var kv in _activeDownloads)
            {
                try { kv.Value.Cts?.Cancel(); } catch { }
            }

            foreach (var offer in _receivedOffers.Values)
            {
                if (_cancelledOffers.ContainsKey(offer.TransferId)) continue;
                foreach (var f in offer.Files)
                {
                    if (_completedFiles.ContainsKey(f.FileId)) continue;

                    int savedChunk = ReadSavedChunkProgress(offer.TransferId, f.FileId);
                    RaiseProgress(offer.TransferId, f.FileId, f.FileName, savedChunk, f.TotalChunks, f.ChunkSize, f.FileSize,
                        TransferStatus.SenderOffline, $"송신자 연결 끊김 ({savedChunk}/{f.TotalChunks} 패킷 저장됨 - 재접속 시 이어받기)", null);
                }
            }
        }

        private async Task RunSequentialDownloadAsync(TransferOffer offer)
        {
            var cts = new CancellationTokenSource();
            var ctx = new ActiveDownloadContext
            {
                TransferId = offer.TransferId,
                Cts = cts
            };
            _activeDownloads[offer.TransferId] = ctx;
            CancellationToken ct = cts.Token;

            await _sequentialDownloadSemaphore.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                foreach (var fileMeta in offer.Files)
                {
                    ct.ThrowIfCancellationRequested();
                    if (_cancelledOffers.ContainsKey(offer.TransferId))
                        break;

                    if (_completedFiles.TryGetValue(fileMeta.FileId, out string existingPath) && File.Exists(existingPath))
                    {
                        RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, fileMeta.TotalChunks, fileMeta.TotalChunks,
                            fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.Completed, "전송 완료", existingPath);
                        continue;
                    }

                    ctx.CurrentFileId = fileMeta.FileId;
                    await DownloadSingleFileOverP2PAsync(offer, fileMeta, ctx, ct).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException)
            {
                // 취소 또는 단절 처리 완료
            }
            catch
            {
                // 개별 파일 다운로드 내부에서 상태 이벤트 발생 완료
            }
            finally
            {
                _sequentialDownloadSemaphore.Release();
                _activeDownloads.TryRemove(offer.TransferId, out _);
            }
        }

        /// <summary>
        /// 단일 파일에 대해 P2P 소켓으로 접속하여:
        /// 1) Handshake 제어 명령 전송
        /// 2) WaitQueue / QueueReady 제어 명령 수신
        /// 3) 기존 조각 이후 번호부터 RequestChunk(패킷 번호) 제어 명령 전송 -> DataChunk 패킷 수신 루프
        /// 4) 복호화 + SHA-256 무결성 확인 후 파일 병합(Merge) 및 FileCompleted 제어 명령 전송
        /// </summary>
        private async Task DownloadSingleFileOverP2PAsync(TransferOffer offer, FileItemMeta fileMeta, ActiveDownloadContext ctx, CancellationToken ct)
        {
            if (!Directory.Exists(DownloadFolder))
            {
                Directory.CreateDirectory(DownloadFolder);
            }

            string partFilePath = GetPartFilePath(offer.TransferId, fileMeta.FileId);
            string idxFilePath = GetIdxFilePath(offer.TransferId, fileMeta.FileId);
            string finalFilePath = Path.Combine(DownloadFolder, fileMeta.FileName);

            // 기존에 받은 패킷(조각) 번호 확인 (이어받기)
            int startChunk = ReadSavedChunkProgress(offer.TransferId, fileMeta.FileId);
            long expectedPartLength = (long)startChunk * fileMeta.ChunkSize;

            if (startChunk > 0 && (!File.Exists(partFilePath) || new FileInfo(partFilePath).Length < expectedPartLength))
            {
                startChunk = 0;
                SaveChunkProgress(idxFilePath, 0);
            }

            ctx.CurrentChunkIndex = startChunk;

            using (var p2pClient = new TcpClient())
            {
                p2pClient.NoDelay = true;
                try
                {
                    await p2pClient.ConnectAsync(offer.SenderIp ?? "127.0.0.1", offer.P2PPort).ConfigureAwait(false);
                    NetworkStream stream = p2pClient.GetStream();
                    ctx.Stream = stream;

                    // 1. Handshake 제어 명령 전송 (헤더 ChunkIndex에 시작 조각 번호 포함)
                    var handshake = new HandshakePayload
                    {
                        TransferId = offer.TransferId,
                        FileId = fileMeta.FileId,
                        PeerId = PeerId,
                        PeerName = PeerName
                    };
                    string hsJson = FrameStreamHelper.SerializeJson(handshake);
                    await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.Handshake, startChunk, hsJson, ct).ConfigureAwait(false);

                    // 2. 송신자로부터 대기열(WaitQueue) 또는 시작 준비(QueueReady) 제어 명령 수신
                    while (true)
                    {
                        P2PFrame ctrlFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                        if (ctrlFrame == null)
                            throw new IOException("송신자 P2P 연결이 끊어졌습니다.");

                        if (ctrlFrame.Command == P2PCommand.WaitQueue)
                        {
                            RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, startChunk, fileMeta.TotalChunks,
                                fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.WaitingQueue, "수신 대기 중 (송신자 동시 전송 제한 대기열)", null);
                            continue;
                        }
                        else if (ctrlFrame.Command == P2PCommand.QueueReady)
                        {
                            break;
                        }
                        else if (ctrlFrame.Command == P2PCommand.Cancel)
                        {
                            HandleCancelledBySender(offer.TransferId);
                            return;
                        }
                        else if (ctrlFrame.Command == P2PCommand.Error)
                        {
                            string reason = ctrlFrame.GetPayloadText();
                            TransferStatus st = reason == "EXPIRED" ? TransferStatus.Expired : TransferStatus.Error;
                            RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, startChunk, fileMeta.TotalChunks,
                                fileMeta.ChunkSize, fileMeta.FileSize, st, $"전송 거부됨 ({reason})", null);
                            return;
                        }
                        else if (ctrlFrame.Command == P2PCommand.Disconnect)
                        {
                            throw new IOException("송신자 네트워크가 종료되었습니다.");
                        }
                    }

                    // 3. 조각 번호(RequestChunk) 요청 및 DataChunk 수신 루프
                    using (var partFs = new FileStream(partFilePath, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None))
                    {
                        partFs.Seek((long)startChunk * fileMeta.ChunkSize, SeekOrigin.Begin);

                        for (int chunkIdx = startChunk; chunkIdx < fileMeta.TotalChunks; chunkIdx++)
                        {
                            ct.ThrowIfCancellationRequested();

                            // 수신자 측에서 일시정지(Pause)를 누른 경우 재개될 때까지 대기
                            while (_pausedOffers.ContainsKey(offer.TransferId) && !ct.IsCancellationRequested && !_cancelledOffers.ContainsKey(offer.TransferId))
                            {
                                await Task.Delay(100, ct).ConfigureAwait(false);
                            }

                            if (_cancelledOffers.ContainsKey(offer.TransferId))
                                throw new OperationCanceledException();

                            ctx.CurrentChunkIndex = chunkIdx;

                            // 3-1. 송신자에게 "chunkIdx번 패킷을 달라"고 RequestChunk 제어 명령 전송
                            await ctx.WriteLock.WaitAsync(ct).ConfigureAwait(false);
                            try
                            {
                                await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.RequestChunk, chunkIdx, null, ct).ConfigureAwait(false);
                            }
                            finally
                            {
                                ctx.WriteLock.Release();
                            }

                            // 3-2. 송신자로부터 패킷 수신 (DataChunk 패킷 또는 중간에 끼어든 Pause/Cancel/Disconnect 제어 명령)
                            P2PFrame responseFrame = null;
                            while (true)
                            {
                                responseFrame = await FrameStreamHelper.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                                if (responseFrame == null)
                                    throw new IOException("패킷 수신 중 송신자 연결이 끊어졌습니다.");

                                if (responseFrame.Command == P2PCommand.DataChunk)
                                {
                                    break;
                                }
                                else if (responseFrame.Command == P2PCommand.Pause)
                                {
                                    _pausedOffers[offer.TransferId] = true;
                                    RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, chunkIdx, fileMeta.TotalChunks,
                                        fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.Paused, $"송신자에 의해 일시정지됨 ({chunkIdx}/{fileMeta.TotalChunks} 패킷)", null);
                                }
                                else if (responseFrame.Command == P2PCommand.Resume)
                                {
                                    _pausedOffers.TryRemove(offer.TransferId, out _);
                                    RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, chunkIdx, fileMeta.TotalChunks,
                                        fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.Transferring, $"송신자에 의해 재개됨 ({chunkIdx}/{fileMeta.TotalChunks} 패킷~)", null);
                                }
                                else if (responseFrame.Command == P2PCommand.Cancel)
                                {
                                    HandleCancelledBySender(offer.TransferId);
                                    return;
                                }
                                else if (responseFrame.Command == P2PCommand.Disconnect)
                                {
                                    throw new IOException("송신자 네트워크가 끊어졌습니다.");
                                }
                                else if (responseFrame.Command == P2PCommand.Error)
                                {
                                    throw new IOException("송신자 오류 응답: " + responseFrame.GetPayloadText());
                                }
                            }

                            byte[] encryptedPayload = responseFrame.Payload ?? new byte[0];

                            // 복호화
                            byte[] decryptedChunk = ChunkSecurityHelper.TransformChunk(encryptedPayload, encryptedPayload.Length, responseFrame.ChunkIndex);

                            // 무결성 검증 (SHA-256)
                            byte[] actualSha256 = ChunkSecurityHelper.ComputeSha256(decryptedChunk, 0, decryptedChunk.Length);
                            if (!ChunkSecurityHelper.VerifyHash(responseFrame.Hash, actualSha256))
                            {
                                throw new InvalidDataException($"패킷 #{responseFrame.ChunkIndex} 무결성 검증(SHA-256) 실패!");
                            }

                            // 디스크 기록 및 완료된 조각 번호 저장
                            if (decryptedChunk.Length > 0)
                            {
                                await partFs.WriteAsync(decryptedChunk, 0, decryptedChunk.Length, ct).ConfigureAwait(false);
                            }
                            await partFs.FlushAsync(ct).ConfigureAwait(false);

                            int completedChunkCount = chunkIdx + 1;
                            ctx.CurrentChunkIndex = completedChunkCount;
                            SaveChunkProgress(idxFilePath, completedChunkCount);

                            // 패킷 크기(fileMeta.ChunkSize)가 일정하므로 완료된 패킷 번호(completedChunkCount)로 진행 상태 계산
                            string resumeTag = startChunk > 0 ? $" [이어받기:{startChunk}번~]" : "";
                            RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, completedChunkCount, fileMeta.TotalChunks,
                                fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.Transferring,
                                $"수신 중 ({completedChunkCount}/{fileMeta.TotalChunks} 패킷){resumeTag}", null);
                        }
                    }

                    // 4. 파일 병합(Merge) 및 최종 확정
                    RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, fileMeta.TotalChunks, fileMeta.TotalChunks,
                        fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.Merging, "무결성 확인 및 파일 병합 중...", null);

                    if (File.Exists(finalFilePath))
                    {
                        try { File.Delete(finalFilePath); } catch { }
                    }

                    File.Move(partFilePath, finalFilePath);
                    if (File.Exists(idxFilePath))
                    {
                        try { File.Delete(idxFilePath); } catch { }
                    }

                    _completedFiles[fileMeta.FileId] = finalFilePath;

                    // 송신자에게 FileCompleted 제어 명령 전송
                    await ctx.WriteLock.WaitAsync(ct).ConfigureAwait(false);
                    try
                    {
                        await FrameStreamHelper.WriteControlFrameAsync(stream, P2PCommand.FileCompleted, fileMeta.TotalChunks, null, ct).ConfigureAwait(false);
                    }
                    finally
                    {
                        ctx.WriteLock.Release();
                    }

                    RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, fileMeta.TotalChunks, fileMeta.TotalChunks,
                        fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.Completed, "수신 완료", finalFilePath);
                }
                catch (OperationCanceledException)
                {
                    if (_cancelledOffers.ContainsKey(offer.TransferId))
                    {
                        CleanupTempFiles(partFilePath, idxFilePath);
                        RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, 0, fileMeta.TotalChunks,
                            fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.Cancelled, "전송 취소됨", null);
                    }
                    else
                    {
                        int saved = ReadSavedChunkProgress(offer.TransferId, fileMeta.FileId);
                        RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, saved, fileMeta.TotalChunks,
                            fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.SenderOffline,
                            $"전송 중단됨 ({saved}/{fileMeta.TotalChunks} 패킷 저장됨 - 이어받기 가능)", null);
                    }
                    throw;
                }
                catch (Exception ex)
                {
                    int saved = ReadSavedChunkProgress(offer.TransferId, fileMeta.FileId);
                    RaiseProgress(offer.TransferId, fileMeta.FileId, fileMeta.FileName, saved, fileMeta.TotalChunks,
                        fileMeta.ChunkSize, fileMeta.FileSize, TransferStatus.SenderOffline,
                        $"연결/전송 오류: {ex.Message} ({saved}/{fileMeta.TotalChunks} 패킷 저장됨 - 재접속 시 이어받기)", null);
                    throw;
                }
            }
        }

        private void HandleCancelledBySender(string transferId)
        {
            _cancelledOffers[transferId] = true;
            _acceptedOffers.TryRemove(transferId, out _);
            _pausedOffers.TryRemove(transferId, out _);

            if (_activeDownloads.TryGetValue(transferId, out var ctx))
            {
                try { ctx.Cts?.Cancel(); } catch { }
            }

            if (_receivedOffers.TryGetValue(transferId, out var offer))
            {
                foreach (var f in offer.Files)
                {
                    CleanupTempFiles(GetPartFilePath(transferId, f.FileId), GetIdxFilePath(transferId, f.FileId));
                    RaiseProgress(transferId, f.FileId, f.FileName, 0, f.TotalChunks, f.ChunkSize, f.FileSize,
                        TransferStatus.Cancelled, "송신자에 의해 전송 취소됨", null);
                }
            }
        }

        /// <summary>
        /// 다운로드 완료된 파일이 로컬 디스크에서 삭제되었는지('파일없음') 및 30분 유효시간 만료 여부를 검사합니다.
        /// </summary>
        public void CheckFilesAndExpirationStatus()
        {
            foreach (var offer in _receivedOffers.Values)
            {
                if (!_acceptedOffers.ContainsKey(offer.TransferId) && !_cancelledOffers.ContainsKey(offer.TransferId) && offer.IsExpired)
                {
                    foreach (var f in offer.Files)
                    {
                        RaiseProgress(offer.TransferId, f.FileId, f.FileName, 0, f.TotalChunks, f.ChunkSize, f.FileSize,
                            TransferStatus.Expired, "유효시간(30분) 만료됨", null);
                    }
                }

                foreach (var f in offer.Files)
                {
                    if (_completedFiles.TryGetValue(f.FileId, out string savedPath))
                    {
                        if (!File.Exists(savedPath))
                        {
                            RaiseProgress(offer.TransferId, f.FileId, f.FileName, f.TotalChunks, f.TotalChunks,
                                f.ChunkSize, f.FileSize, TransferStatus.FileMissing, "파일없음", savedPath);
                        }
                    }
                }
            }
        }

        private string GetPartFilePath(string transferId, string fileId)
        {
            return Path.Combine(DownloadFolder, $".p2p_{transferId}_{fileId}.part");
        }

        private string GetIdxFilePath(string transferId, string fileId)
        {
            return Path.Combine(DownloadFolder, $".p2p_{transferId}_{fileId}.idx");
        }

        public int ReadSavedChunkProgress(string transferId, string fileId)
        {
            try
            {
                string idxPath = GetIdxFilePath(transferId, fileId);
                if (File.Exists(idxPath))
                {
                    string text = File.ReadAllText(idxPath).Trim();
                    if (int.TryParse(text, out int idx) && idx >= 0)
                        return idx;
                }
            }
            catch { }
            return 0;
        }

        private void SaveChunkProgress(string idxFilePath, int completedChunks)
        {
            try
            {
                File.WriteAllText(idxFilePath, completedChunks.ToString());
            }
            catch { }
        }

        private void CleanupTempFiles(string partPath, string idxPath)
        {
            try { if (File.Exists(partPath)) File.Delete(partPath); } catch { }
            try { if (File.Exists(idxPath)) File.Delete(idxPath); } catch { }
        }

        private void RaiseProgress(string transferId, string fileId, string fileName,
            int completedChunkCount, int totalChunks, int chunkSize, long totalBytes,
            TransferStatus status, string message, string savedFilePath)
        {
            // 패킷 크기(chunkSize)가 일정하므로 완료된 패킷 번호(completedChunkCount)로 전송 바이트 산출
            long transferredBytes = Math.Min(totalBytes, (long)completedChunkCount * chunkSize);

            TransferProgressUpdated?.Invoke(this, new TransferProgressEventArgs
            {
                TransferId = transferId,
                FileId = fileId,
                FileName = fileName,
                PeerId = PeerId,
                PeerName = PeerName,
                CurrentChunkIndex = completedChunkCount,
                TotalChunks = totalChunks,
                ChunkSize = chunkSize,
                TransferredBytes = transferredBytes,
                TotalBytes = totalBytes,
                Status = status,
                Message = message,
                SavedFilePath = savedFilePath
            });
        }

        public void Dispose()
        {
            foreach (var kv in _activeDownloads)
            {
                try { kv.Value.Cts?.Cancel(); } catch { }
            }
            _activeDownloads.Clear();
        }
    }
}
