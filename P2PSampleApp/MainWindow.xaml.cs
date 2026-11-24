using System;
using System.IO;
using System.Windows;
using Microsoft.Win32;
using P2PTransferLib;

namespace P2PSampleApp
{
    /// <summary>
    /// 뷰모델/채팅 없이 글로벌 P2PSenderNode.Instance 와 자동 리스트 관리형 P2PReceiverNode.Instance 를
    /// 호출하고 송신/수신 목록 조회(상태 및 유효시간 확인)와 로그 출력을 확인하는 샘플 코드
    /// </summary>
    public partial class MainWindow : Window
    {
        // 1. 송신자(Sender)는 글로벌 싱글톤(또는 전역 변수) 1개만 사용
        private readonly P2PSenderNode _sender = P2PSenderNode.Instance;

        // 2. 수신자(Receiver) 매니저는 여러 수신 작업(P2PReceiverItem)을 내부 리스트로 자동 추가/관리
        private readonly P2PReceiverNode _receiverManager = P2PReceiverNode.Instance;

        // 3. 파일 삭제/복구 감시(FileSystemWatcher) 전용 독립 매니저 클래스
        private readonly P2PFileWatchManager _fileWatcher = P2PFileWatchManager.Instance;

        public MainWindow()
        {
            InitializeComponent();

            TxtDownloadFolder.Text = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Downloads");

            // 송신자(서버) 로그 및 OnProgress 이벤트 연결
            _sender.OnLog += msg =>
            {
                Dispatcher.BeginInvoke(new Action(() => AppendSenderLog(msg)));
            };

            _sender.OnProgress += (s, e) =>
            {
                if (e.Status != TransferStatus.Transferring)
                {
                    Dispatcher.BeginInvoke(new Action(() =>
                    {
                        AppendSenderLog($"   -> [OnProgress] 상태={e.Status} | 수신자={e.PeerName}({e.PeerId}) | chID={e.ChId}, msgID={e.MsgId} | 진행률={e.ProgressPercentage:0.0}% ({e.CurrentChunkIndex}/{e.TotalChunks}) | 남은유효시간={e.RemainingValidTime:mm\\:ss} | {e.Message}");
                    }));
                }
            };

            // 수신자(클라이언트) 로그 및 OnProgress 이벤트 연결
            _receiverManager.OnLog += msg =>
            {
                Dispatcher.BeginInvoke(new Action(() => AppendReceiverLog(msg)));
            };

            _receiverManager.OnProgress += (s, e) =>
            {
                if (e.Status != TransferStatus.Transferring)
                {
                    Dispatcher.BeginInvoke(new Action(() =>
                    {
                        AppendReceiverLog($"   -> [OnProgress] 상태={e.Status} | 사번={e.PeerId}({e.PeerName}) | chID={e.ChId}, msgID={e.MsgId} | 진행률={e.ProgressPercentage:0.0}% ({e.CurrentChunkIndex}/{e.TotalChunks}) | 남은유효시간={e.RemainingValidTime:mm\\:ss} | {e.Message}");
                    }));

                    // 수신 완료 시 독립된 Watch 클래스(P2PFileWatchManager)에 해당 파일을 등록하여 삭제 여부 감시
                    if (e.Status == TransferStatus.Completed && !string.IsNullOrEmpty(e.SavedFilePath))
                    {
                        _fileWatcher.RegisterWatch(e.ChId, e.MsgId, e.SavedFilePath, "Receiver");
                    }
                }
            };

            // 독립된 파일 감시 클래스(P2PFileWatchManager) 이벤트 연결
            _fileWatcher.OnLog += msg =>
            {
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    AppendSenderLog(msg);
                    AppendReceiverLog(msg);
                }));
            };

            _fileWatcher.OnFileMissing += (s, e) =>
            {
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    string alert = $"   -> [P2PFileWatchManager.OnFileMissing 이벤트] 상태={e.Status} | chID={e.ChId}, msgID={e.MsgId} ({e.FileName}) | {e.Message}";
                    AppendSenderLog(alert);
                    AppendReceiverLog(alert);
                }));
            };

            AppendSenderLog("=== [안내] 1. '서버 시작(포트 오픈)' 클릭 -> 2. '파일 + chID/msgID 등록' 클릭 ===");
            AppendReceiverLog("=== [안내] 서버 주소, 사번(peerID), 이름(peerName), 채널ID(chID), 파일ID(msgID) 입력 후 '수신 시작' 클릭 ===");
        }

        // =====================================================================
        // [1] 송신자 (서버) API 호출부 (글로벌 1개 인스턴스)
        // =====================================================================

        private void BtnStartServer_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                int port = int.TryParse(TxtServerPort.Text.Trim(), out int p) ? p : 9002;
                string bindIp = TxtServerBindIp.Text.Trim();

                ApplyServerOptions();
                _sender.StartServer(port, bindIp);
            }
            catch (Exception ex)
            {
                AppendSenderLog("[오류] " + ex.Message);
            }
        }

        private void BtnStopServer_Click(object sender, RoutedEventArgs e)
        {
            _sender.StopServer();
        }

        private void BtnApplyServerOptions_Click(object sender, RoutedEventArgs e)
        {
            ApplyServerOptions();
            AppendSenderLog($"[옵션 적용] 동시전송 제한={_sender.MaxConcurrentReceivers}개, 속도제한={_sender.SpeedLimitKbps} KB/s");
        }

        private void ApplyServerOptions()
        {
            if (int.TryParse(TxtMaxReceivers.Text.Trim(), out int maxR))
                _sender.MaxConcurrentReceivers = maxR;
            if (int.TryParse(TxtSpeedLimit.Text.Trim(), out int spd))
                _sender.SpeedLimitKbps = spd;
        }

        private void BtnBrowseFile_Click(object sender, RoutedEventArgs e)
        {
            var dlg = new OpenFileDialog { Title = "등록할 파일 선택" };
            if (dlg.ShowDialog() == true)
            {
                TxtRegisterFilePath.Text = dlg.FileName;
            }
        }

        private void BtnRegisterFile_Click(object sender, RoutedEventArgs e)
        {
            string chId = TxtRegisterChId.Text.Trim();
            string msgId = TxtRegisterMsgId.Text.Trim();
            string filePath = TxtRegisterFilePath.Text.Trim();

            // 없는 파일이나 잘못된 값, 또는 (chId, msgId, filePath)가 동일한 중복 아이템이면 false 반환
            if (_sender.TryRegisterFile(chId, msgId, filePath, out var item, out _))
            {
                // 독립된 Watch 매니저에 감시 등록
                _fileWatcher.RegisterWatch(item.ChId, item.MsgId, item.LocalSourcePath, "Sender");
            }
        }

        private void BtnUnregisterFile_Click(object sender, RoutedEventArgs e)
        {
            string chId = TxtRegisterChId.Text.Trim();
            string msgId = TxtRegisterMsgId.Text.Trim();
            string filePath = TxtRegisterFilePath.Text.Trim();

            // 송신 파일 등록 해제 (UnregisterFile) 및 독립 Watch 해제 (UnregisterWatch)
            _sender.UnregisterFile(chId, msgId);
            _fileWatcher.UnregisterWatch(chId, msgId, string.IsNullOrWhiteSpace(filePath) ? null : filePath);
        }

        private void BtnCreateDummyAndRegister_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (!_sender.IsRunning)
                {
                    BtnStartServer_Click(null, null);
                }

                string sampleDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SampleFiles");
                if (!Directory.Exists(sampleDir)) Directory.CreateDirectory(sampleDir);

                string chId = TxtRegisterChId.Text.Trim();

                // 테스트용 더미 파일 5개(SampleData_1.bin ~ SampleData_5.bin) 생성 및 MSG_1001 ~ MSG_1005 등록
                for (int i = 1; i <= 5; i++)
                {
                    string file = Path.Combine(sampleDir, $"SampleData_{i}.bin");
                    CreateDummyFile(file, 1536 * 1024); // 각 1.5MB (24패킷)
                    if (i == 1) TxtRegisterFilePath.Text = file;

                    if (_sender.TryRegisterFile(chId, $"MSG_100{i}", file, out var item, out _))
                    {
                        _fileWatcher.RegisterWatch(item.ChId, item.MsgId, item.LocalSourcePath, "Sender");
                    }
                }
            }
            catch (Exception ex)
            {
                AppendSenderLog("[더미 생성 오류] " + ex.Message);
            }
        }

        private void CreateDummyFile(string path, int sizeBytes)
        {
            if (File.Exists(path) && new FileInfo(path).Length == sizeBytes) return;
            byte[] buf = new byte[sizeBytes];
            new Random(42).NextBytes(buf);
            File.WriteAllBytes(path, buf);
        }

        /// <summary>
        /// [송신파일 목록 조회 코드]
        /// 1) 등록된 송신 파일 목록(GetRegisteredFiles)의 상태(Ready/Expired/FileMissing) 및 남은 유효시간 확인
        /// 2) 각 수신자별 송신 상태 목록(GetSenderTransferList)의 상태(WaitingQueue/Transferring/Completed/Cancelled 등) 및 유효시간 확인
        /// </summary>
        private void BtnShowSenderList_Click(object sender, RoutedEventArgs e)
        {
            var registeredFiles = _sender.GetRegisteredFiles();
            AppendSenderLog($"=== [1. 등록된 송신 파일 목록: 총 {registeredFiles.Count}개] ===");
            foreach (var f in registeredFiles)
            {
                AppendSenderLog($"   * chID:{f.ChId} | msgID:{f.MsgId} | 파일명:{f.FileName} ({P2PSenderNode.FormatBytes(f.FileSize)}) | 상태:{f.Status} | 유효시간:{f.RemainingValidTimeText} (만료여부:{f.IsExpired})");
            }

            var transferList = _sender.GetSenderTransferList();
            AppendSenderLog($"=== [2. 수신자별 송신 진행/완료 목록: 총 {transferList.Count}건] ===");
            foreach (var t in transferList)
            {
                AppendSenderLog($"   * 수신자:{t.PeerName}({t.PeerId}) | chID:{t.ChId} | msgID:{t.MsgId} ({t.FileName}) | 상태:{t.Status} | 진행률:{t.ProgressPercentage:0.0}% ({t.CurrentChunkIndex}/{t.TotalChunks}) | 유효시간:{t.RemainingValidTimeText}");
            }
        }

        private void BtnCancelSenderTransfer_Click(object sender, RoutedEventArgs e)
        {
            _sender.CancelTransfer(TxtRegisterChId.Text.Trim(), TxtRegisterMsgId.Text.Trim());
        }

        private void BtnDeleteSenderFileTest_Click(object sender, RoutedEventArgs e)
        {
            string chId = TxtRegisterChId.Text.Trim();
            string msgId = TxtRegisterMsgId.Text.Trim();
            var list = _sender.GetRegisteredFiles(chId);
            var target = list.Find(x => x.MsgId == msgId) ?? (list.Count > 0 ? list[0] : null);

            if (target != null && File.Exists(target.LocalSourcePath))
            {
                File.Delete(target.LocalSourcePath);
                // FileSystemWatcher 및 1초 백그라운드 타이머가 자동으로 파일 삭제를 감지하여 OnFileWatchAlert & 로그를 출력함
            }
            else
            {
                AppendSenderLog("[안내] 삭제 테스트할 등록된 원본 파일이 존재하지 않습니다.");
            }
        }

        private void BtnClearSenderLog_Click(object sender, RoutedEventArgs e)
        {
            TxtSenderLog.Clear();
        }

        // =====================================================================
        // [2] 수신자 (클라이언트) API 호출부 (내부 리스트로 여러 Receiver 자동 관리)
        // =====================================================================

        private void BtnStartReceive_Click(object sender, RoutedEventArgs e)
        {
            string serverIp = TxtTargetServerIp.Text.Trim();
            int serverPort = int.TryParse(TxtTargetServerPort.Text.Trim(), out int p) ? p : -1;
            string peerId = TxtPeerId.Text.Trim();       // 사번
            string peerName = TxtPeerName.Text.Trim();   // 이름
            string chId = TxtRequestChId.Text.Trim();    // 채널ID (chID)
            string msgId = TxtRequestMsgId.Text.Trim();  // 파일ID (msgID)
            string folder = TxtDownloadFolder.Text.Trim();

            // 잘못된 포트/IP, 미등록 msgID, 네트워크 미연결 등 모든 오류가 내부에서 안전하게 처리되고 로그로 출력됨
            _ = _receiverManager.StartDownloadAsync(serverIp, serverPort, peerId, peerName, chId, msgId, folder);
        }

        /// <summary>
        /// [기존에 받다가 중지된 파일이 있다면 몇 퍼센트 받았었는지 확인하는 코드]
        /// 수신 시작을 누르기 전이라도 지정한 chID/msgID(또는 파일명)에 대해 로컬에 받다 중지된 .part 파일이 있는지,
        /// 몇 퍼센트(SavedPercentage) 받았었는지 즉시 확인합니다.
        /// </summary>
        private async void BtnCheckPartialProgress_Click(object sender, RoutedEventArgs e)
        {
            string serverIp = TxtTargetServerIp.Text.Trim();
            int serverPort = int.TryParse(TxtTargetServerPort.Text.Trim(), out int p) ? p : -1;
            string peerId = TxtPeerId.Text.Trim();
            string peerName = TxtPeerName.Text.Trim();
            string chId = TxtRequestChId.Text.Trim();
            string msgId = TxtRequestMsgId.Text.Trim();
            string folder = TxtDownloadFolder.Text.Trim();

            PartialDownloadCheckResult check = await _receiverManager.CheckPartialProgressFromServerAsync(
                serverIp, serverPort, peerId, peerName, chId, msgId, folder);

            AppendReceiverLog($"   -> [사전 확인 결과] 파일명:{check.FileName ?? "(미확인)"} | 중지된파일존재:{check.HasPartialFile} | 기존진행률:{check.SavedPercentage:0.0}% ({check.SavedChunkCount}/{check.TotalChunks} 패킷) | {check.SummaryMessage}");
        }

        private void BtnTestMultiConcurrentReceive_Click(object sender, RoutedEventArgs e)
        {
            if (!_sender.IsRunning || _sender.GetRegisteredFiles().Count < 5)
            {
                BtnCreateDummyAndRegister_Click(null, null);
            }

            string serverIp = TxtTargetServerIp.Text.Trim();
            int serverPort = int.TryParse(TxtTargetServerPort.Text.Trim(), out int p) ? p : 9002;
            string chId = TxtRequestChId.Text.Trim();
            string folder = TxtDownloadFolder.Text.Trim();

            // 5개의 파일(MSG_1001 ~ MSG_1005)을 같은 폴더에 원래 파일명(SampleData_1.bin ~ SampleData_5.bin) 그대로 동시 수신
            // (동시제한 3개는 즉시 전송되고, 나머지 2개는 대기열 후 자동 전송됨)
            _ = _receiverManager.StartDownloadAsync(serverIp, serverPort, "EMP_2001", "홍길동", chId, "MSG_1001", folder);
            _ = _receiverManager.StartDownloadAsync(serverIp, serverPort, "EMP_2002", "김철수", chId, "MSG_1002", folder);
            _ = _receiverManager.StartDownloadAsync(serverIp, serverPort, "EMP_2003", "이영희", chId, "MSG_1003", folder);
            _ = _receiverManager.StartDownloadAsync(serverIp, serverPort, "EMP_2004", "박민수", chId, "MSG_1004", folder);
            _ = _receiverManager.StartDownloadAsync(serverIp, serverPort, "EMP_2005", "최지훈", chId, "MSG_1005", folder);
        }

        /// <summary>
        /// [수신 목록 조회 코드]
        /// 자동 관리되는 Receiver 목록(GetReceiverList)을 조회하여 각 항목의 상태(Status - 완료 후 파일 삭제 시 FileMissing 자동 감지)와
        /// 남은 유효시간(RemainingValidTimeText, IsExpired) 및 진행률(%)을 확인합니다.
        /// </summary>
        private void BtnShowReceiverList_Click(object sender, RoutedEventArgs e)
        {
            var allList = _receiverManager.GetReceiverList();
            var activeList = _receiverManager.GetActiveReceiverList();

            AppendReceiverLog($"=== [수신 목록 조회: 전체 {allList.Count}개 / 현재 진행중 {activeList.Count}개] ===");
            foreach (var item in allList)
            {
                AppendReceiverLog($"   * 사번:{item.PeerId}({item.PeerName}) | chID:{item.ChId} | msgID:{item.MsgId} ({item.FileName}) | 상태:{item.Status} | 진행률:{item.ProgressPercentage:0.0}% ({item.CurrentChunkIndex}/{item.TotalChunks}) | 유효시간:{item.RemainingValidTimeText}");
            }
        }

        private void BtnPauseReceive_Click(object sender, RoutedEventArgs e)
        {
            _receiverManager.PauseDownload(TxtRequestChId.Text.Trim(), TxtRequestMsgId.Text.Trim(), TxtPeerId.Text.Trim());
        }

        private void BtnResumeReceive_Click(object sender, RoutedEventArgs e)
        {
            _receiverManager.ResumeDownload(TxtRequestChId.Text.Trim(), TxtRequestMsgId.Text.Trim(), TxtPeerId.Text.Trim());
        }

        private void BtnCancelReceive_Click(object sender, RoutedEventArgs e)
        {
            _receiverManager.CancelDownload(TxtRequestChId.Text.Trim(), TxtRequestMsgId.Text.Trim(), TxtPeerId.Text.Trim());
        }

        private void BtnDeleteReceivedFileTest_Click(object sender, RoutedEventArgs e)
        {
            var list = _receiverManager.GetReceiverList();
            var target = list.Find(x => !string.IsNullOrEmpty(x.SavedFilePath) && File.Exists(x.SavedFilePath))
                      ?? list.Find(x => !string.IsNullOrEmpty(x.PartFilePath) && File.Exists(x.PartFilePath));

            if (target != null)
            {
                if (!string.IsNullOrEmpty(target.SavedFilePath) && File.Exists(target.SavedFilePath))
                {
                    File.Delete(target.SavedFilePath);
                }
                else if (!string.IsNullOrEmpty(target.PartFilePath) && File.Exists(target.PartFilePath))
                {
                    File.Delete(target.PartFilePath);
                }
                // FileSystemWatcher 및 1초 백그라운드 타이머가 자동으로 파일 삭제를 감지하여 OnFileWatchAlert & 로그를 출력함
            }
            else
            {
                AppendReceiverLog("[안내] 삭제 테스트할 수신 완료된 파일(또는 중지된 .part 파일)이 없습니다.");
            }
        }

        private void BtnClearReceiverLog_Click(object sender, RoutedEventArgs e)
        {
            TxtReceiverLog.Clear();
        }

        private void AppendSenderLog(string text)
        {
            TxtSenderLog.AppendText(text + Environment.NewLine);
            TxtSenderLog.ScrollToEnd();
        }

        private void AppendReceiverLog(string text)
        {
            TxtReceiverLog.AppendText(text + Environment.NewLine);
            TxtReceiverLog.ScrollToEnd();
        }

        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            _fileWatcher.Dispose();
            _receiverManager.Dispose();
            _sender.Dispose();
        }
    }
}
