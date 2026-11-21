using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Threading;
using Microsoft.Win32;
using P2PTransferLib;

namespace P2PSampleApp
{
    public partial class MainWindow : Window
    {
        // 샘플 프로그램 자체 채팅 서버 (채팅은 최초 파일명/크기 알림만 수행)
        private SampleChatServer _chatServer;

        // P2P 전송 동적 라이브러리(DLL)의 송신자 엔진 (P2P 포트로 모든 제어 명령 및 패킷 전송/진행상태 처리)
        private P2PSenderNode _p2pSender;

        private PeerNodeViewModel _senderPeerVm;

        public ObservableCollection<PeerNodeViewModel> PeerNodes { get; } = new ObservableCollection<PeerNodeViewModel>();
        public ObservableCollection<PeerNodeViewModel> ReceiverNodesOnly { get; } = new ObservableCollection<PeerNodeViewModel>();
        public ObservableCollection<SenderTransferItemViewModel> SenderTransfers { get; } = new ObservableCollection<SenderTransferItemViewModel>();

        private DispatcherTimer _uiTimer;
        private int _nextClientCharIndex = 0;
        private readonly string[] _clientNames = { "B", "C", "D", "E", "F", "G", "H", "I", "J" };

        public MainWindow()
        {
            InitializeComponent();

            GridSenderTransfers.ItemsSource = SenderTransfers;
            LstPeers.ItemsSource = PeerNodes;
            CmbReceiverFilter.ItemsSource = ReceiverNodesOnly;

            InitializeNodes();
            StartBackgroundCheckerTimer();
        }

        private void InitializeNodes()
        {
            string serverIp = TxtServerIp.Text.Trim();
            int chatPort = int.TryParse(TxtChatPort.Text, out int cp) ? cp : 9001;
            int p2pPort = int.TryParse(TxtP2PPort.Text, out int pp) ? pp : 9002;

            // 1. 샘플 프로그램의 채팅 서버 초기화
            _chatServer = new SampleChatServer();
            _chatServer.MessageReceived += ChatServer_MessageReceived;

            // 2. P2PTransferLib(DLL)의 P2P 송신 엔진 초기화
            _p2pSender = new P2PSenderNode
            {
                NodeName = "A (P2P 송신서버)",
                MaxConcurrentReceivers = int.TryParse(TxtMaxReceivers.Text, out int mr) ? mr : 3,
                SpeedLimitKbps = int.TryParse(TxtSpeedLimit.Text, out int sl) ? sl : 2048
            };
            _p2pSender.TransferProgressUpdated += P2PSender_TransferProgressUpdated;

            try
            {
                // serverIp가 127.0.0.1이면 Loopback으로 리스닝하여 Windows 방화벽 팝업이 뜨지 않음
                _chatServer.Start(chatPort, serverIp);
                _p2pSender.OpenP2PServer(p2pPort, serverIp);
            }
            catch (Exception ex)
            {
                MessageBox.Show($"포트({chatPort}/{p2pPort}) 시작 오류: {ex.Message}", "오류", MessageBoxButton.OK, MessageBoxImage.Warning);
            }

            _senderPeerVm = new PeerNodeViewModel
            {
                PeerId = _chatServer.ServerId,
                DisplayName = "A : P2P 송신자 (서버)",
                IsSender = true,
                IsConnected = true
            };
            _senderPeerVm.ChatLogs.Add($"[{DateTime.Now:HH:mm:ss}] [시스템] 채팅 서버(포트:{chatPort}) 및 P2P 엔진 포트({p2pPort})가 준비되었습니다.");
            PeerNodes.Add(_senderPeerVm);

            // 3. 기본 수신자 클라이언트 B, C, D, E, F 생성
            for (int i = 0; i < 5; i++)
            {
                CreateAndRegisterReceiverClient();
            }

            LstPeers.SelectedIndex = 0;
            if (ReceiverNodesOnly.Count > 0)
            {
                CmbReceiverFilter.SelectedIndex = 0;
            }
        }

        private PeerNodeViewModel CreateAndRegisterReceiverClient()
        {
            string label = _nextClientCharIndex < _clientNames.Length
                ? _clientNames[_nextClientCharIndex]
                : $"Client_{_nextClientCharIndex + 1}";
            _nextClientCharIndex++;

            string peerId = $"PEER_{label}";
            string peerName = $"{label} (수신자)";
            string defaultFolder = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Downloads", $"Client_{label}");

            var chatClient = new SampleChatClient(peerId, peerName);
            var p2pReceiver = new P2PReceiverNode(peerId, peerName, defaultFolder);

            var vm = new PeerNodeViewModel
            {
                PeerId = peerId,
                DisplayName = $"{label} : P2P 수신자 (클라이언트)",
                IsSender = false,
                IsConnected = false,
                ChatClient = chatClient,
                ReceiverInstance = p2pReceiver,
                DownloadFolder = defaultFolder
            };

            vm.ChatLogs.Add($"[{DateTime.Now:HH:mm:ss}] [안내] '채팅 시작(접속)' 버튼을 누르면 채팅 서버에 연결됩니다.");

            // 채팅 메시지 수신 (최초 파일 전송 알림 "FILE_NOTICE" 포함)
            chatClient.MessageReceived += (s, e) =>
            {
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    vm.ChatLogs.Add($"[{e.Timestamp:HH:mm:ss}] [{e.SenderName}] {e.Message}");

                    // 채팅으로 최초 "파일 보낼 것이 있다(파일명/크기)"고 받았을 때 P2PReceiverNode에 등록
                    if (e.Type == "FILE_NOTICE" && e.FileNotice != null)
                    {
                        foreach (var f in e.FileNotice.Files)
                        {
                            if (!vm.ReceivedFiles.Any(x => x.TransferId == e.FileNotice.TransferId && x.FileId == f.FileId))
                            {
                                var item = new ReceiverTransferItemViewModel
                                {
                                    OwnerPeerId = vm.PeerId,
                                    OwnerPeerName = label,
                                    TransferId = e.FileNotice.TransferId,
                                    FileId = f.FileId,
                                    FileName = f.FileName,
                                    TotalBytes = f.FileSize,
                                    TotalChunks = f.TotalChunks,
                                    CurrentChunk = 0,
                                    Progress = 0,
                                    P2PPort = e.FileNotice.P2PPort,
                                    ExpireTimeUtc = e.FileNotice.ExpireTimeUtc,
                                    Status = TransferStatus.PendingAccept,
                                    StatusMessage = "수신 수락 대기 중 (30분 유효)"
                                };
                                item.UpdateRemainingTime();
                                vm.ReceivedFiles.Add(item);
                            }
                        }

                        // P2P 라이브러리 엔진에 해당 전송 메타데이터 등록
                        p2pReceiver.RegisterTransferOffer(e.FileNotice);
                    }
                    else if (e.Type == "PRE_CANCEL" && !string.IsNullOrEmpty(e.TransferId))
                    {
                        // 수신 시작(P2P 연결 수립) 전에 송신자가 채팅으로 송신 취소를 알려온 경우
                        p2pReceiver.NotifyCancelledBySenderBeforeConnection(e.TransferId);
                    }
                }));
            };

            chatClient.ConnectionStateChanged += (s, connected) =>
            {
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    vm.IsConnected = connected;
                    if (!connected)
                    {
                        vm.ChatLogs.Add($"[{DateTime.Now:HH:mm:ss}] [경고] 송신자 네트워크 연결이 끊어졌습니다!");
                        p2pReceiver.NotifySenderOffline();
                    }
                }));
            };

            // P2P 포트에서 발생하는 모든 진행 상태 업데이트 (패킷 번호 기반 진행률, 대기열, 일시정지, 취소, 병합 완료 등)
            p2pReceiver.TransferProgressUpdated += (s, e) =>
            {
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    var item = vm.ReceivedFiles.FirstOrDefault(x => x.TransferId == e.TransferId && x.FileId == e.FileId);
                    if (item == null)
                    {
                        item = new ReceiverTransferItemViewModel
                        {
                            OwnerPeerId = vm.PeerId,
                            OwnerPeerName = label,
                            TransferId = e.TransferId,
                            FileId = e.FileId,
                            FileName = e.FileName,
                            TotalBytes = e.TotalBytes,
                            TotalChunks = e.TotalChunks,
                            ExpireTimeUtc = DateTime.UtcNow.AddMinutes(30)
                        };
                        vm.ReceivedFiles.Add(item);
                    }

                    item.CurrentChunk = e.CurrentChunkIndex;
                    item.TotalChunks = e.TotalChunks;
                    item.Progress = e.ProgressPercentage;
                    item.Status = e.Status;
                    item.StatusMessage = e.Message;
                    if (!string.IsNullOrEmpty(e.SavedFilePath))
                    {
                        item.SavedFilePath = e.SavedFilePath;
                    }
                }));
            };

            PeerNodes.Add(vm);
            ReceiverNodesOnly.Add(vm);
            return vm;
        }

        private void ChatServer_MessageReceived(object sender, SampleChatEventArgs e)
        {
            Dispatcher.BeginInvoke(new Action(() =>
            {
                _senderPeerVm.ChatLogs.Add($"[{e.Timestamp:HH:mm:ss}] [{e.SenderName}] {e.Message}");

                // 수신자가 수신 시작(P2P 연결 수립)을 누르기 전에 채팅으로 수신 취소(거절)를 보낸 경우
                if (e.Type == "PRE_CANCEL" && !string.IsNullOrEmpty(e.TransferId))
                {
                    _p2pSender.NotifyCancelledByPeerBeforeConnection(e.TransferId, e.SenderId, e.SenderName);
                }
            }));
        }

        private void P2PSender_TransferProgressUpdated(object sender, TransferProgressEventArgs e)
        {
            Dispatcher.BeginInvoke(new Action(() =>
            {
                var item = SenderTransfers.FirstOrDefault(x => x.TransferId == e.TransferId && x.FileId == e.FileId && x.PeerId == e.PeerId);
                if (item == null)
                {
                    item = new SenderTransferItemViewModel
                    {
                        TransferId = e.TransferId,
                        FileId = e.FileId,
                        FileName = e.FileName,
                        PeerId = e.PeerId,
                        PeerName = e.PeerName,
                        TotalBytes = e.TotalBytes,
                        TotalChunks = e.TotalChunks
                    };
                    SenderTransfers.Add(item);
                }

                item.CurrentChunk = e.CurrentChunkIndex;
                item.TotalChunks = e.TotalChunks;
                item.Progress = e.ProgressPercentage;
                item.Status = e.Status;
                item.StatusMessage = e.Message;
            }));
        }

        /// <summary>
        /// 1초마다:
        /// 1) 30분 유효시간 카운트다운 업데이트 및 만료 처리
        /// 2) 다운로드 완료된 파일이 디스크에서 삭제되었는지 확인하여 '파일없음' 표기
        /// </summary>
        private void StartBackgroundCheckerTimer()
        {
            _uiTimer = new DispatcherTimer
            {
                Interval = TimeSpan.FromSeconds(1)
            };
            _uiTimer.Tick += (s, e) =>
            {
                foreach (var rNode in ReceiverNodesOnly)
                {
                    rNode.ReceiverInstance?.CheckFilesAndExpirationStatus();
                    foreach (var fileItem in rNode.ReceivedFiles)
                    {
                        fileItem.UpdateRemainingTime();
                    }
                }
            };
            _uiTimer.Start();
        }

        private void LstPeers_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (LstPeers.SelectedItem is PeerNodeViewModel selected)
            {
                TxtActiveChatHeader.Text = $"채팅 로그 [{selected.DisplayName}]";
                LstChatMessages.ItemsSource = selected.ChatLogs;
                BtnSelectAndSendFiles.IsEnabled = selected.IsSender;

                if (!selected.IsSender)
                {
                    CmbReceiverFilter.SelectedItem = selected;
                }
            }
        }

        private void CmbReceiverFilter_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (CmbReceiverFilter.SelectedItem is PeerNodeViewModel selectedReceiver)
            {
                GridReceiverTransfers.ItemsSource = selectedReceiver.ReceivedFiles;
                TxtDownloadFolder.Text = selectedReceiver.DownloadFolder;
            }
        }

        private async void BtnToggleNodeConnection_Click(object sender, RoutedEventArgs e)
        {
            if (!((sender as Button)?.Tag is PeerNodeViewModel vm)) return;

            int chatPort = int.TryParse(TxtChatPort.Text, out int cp) ? cp : 9001;
            int p2pPort = int.TryParse(TxtP2PPort.Text, out int pp) ? pp : 9002;
            string serverIp = TxtServerIp.Text.Trim();

            if (vm.IsSender)
            {
                if (vm.IsConnected)
                {
                    _p2pSender.StopP2PServer();
                    _chatServer.Stop();
                    vm.IsConnected = false;
                    vm.ChatLogs.Add($"[{DateTime.Now:HH:mm:ss}] [시스템] 송신자 네트워크(채팅+P2P 서버)가 중지되었습니다. 수신자들에게 끊김이 전달됩니다.");
                }
                else
                {
                    _chatServer.Start(chatPort, serverIp);
                    _p2pSender.OpenP2PServer(p2pPort, serverIp);
                    vm.IsConnected = true;
                    vm.ChatLogs.Add($"[{DateTime.Now:HH:mm:ss}] [시스템] 송신자 서버가 재시작되었습니다. 수신자가 재접속하면 P2P 포트로 이어받기가 진행됩니다.");
                }
            }
            else
            {
                if (vm.IsConnected)
                {
                    vm.ChatClient.Disconnect();
                    vm.IsConnected = false;
                    vm.ChatLogs.Add($"[{DateTime.Now:HH:mm:ss}] [시스템] 채팅 연결을 종료했습니다.");
                }
                else
                {
                    try
                    {
                        await vm.ChatClient.ConnectAsync(serverIp, chatPort);
                        vm.IsConnected = true;
                        // 재접속 시 미완료된 P2P 전송이 있다면 자동으로 이어받기 요청
                        await vm.ReceiverInstance.AutoResumeIncompleteTransfersAsync(serverIp, p2pPort);
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show($"{vm.DisplayName} 접속 실패: {ex.Message}", "접속 오류", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
            }
        }

        private async void BtnConnectAllClients_Click(object sender, RoutedEventArgs e)
        {
            int chatPort = int.TryParse(TxtChatPort.Text, out int cp) ? cp : 9001;
            int p2pPort = int.TryParse(TxtP2PPort.Text, out int pp) ? pp : 9002;
            string serverIp = TxtServerIp.Text.Trim();

            if (!_senderPeerVm.IsConnected)
            {
                _chatServer.Start(chatPort, serverIp);
                _p2pSender.OpenP2PServer(p2pPort, serverIp);
                _senderPeerVm.IsConnected = true;
            }

            foreach (var rVm in ReceiverNodesOnly)
            {
                if (!rVm.IsConnected)
                {
                    try
                    {
                        await rVm.ChatClient.ConnectAsync(serverIp, chatPort);
                        rVm.IsConnected = true;
                        await rVm.ReceiverInstance.AutoResumeIncompleteTransfersAsync(serverIp, p2pPort);
                    }
                    catch { }
                }
            }
        }

        private void BtnAddClientNode_Click(object sender, RoutedEventArgs e)
        {
            var newVm = CreateAndRegisterReceiverClient();
            CmbReceiverFilter.SelectedItem = newVm;
        }

        private void BtnApplyOptions_Click(object sender, RoutedEventArgs e)
        {
            if (int.TryParse(TxtSpeedLimit.Text.Trim(), out int speed))
            {
                _p2pSender.SpeedLimitKbps = Math.Max(0, speed);
            }
            if (int.TryParse(TxtMaxReceivers.Text.Trim(), out int maxRecv))
            {
                _p2pSender.MaxConcurrentReceivers = Math.Max(1, maxRecv);
            }

            _senderPeerVm.ChatLogs.Add($"[{DateTime.Now:HH:mm:ss}] [P2P 설정 변경] 속도제한={_p2pSender.SpeedLimitKbps} KB/s, 최대 동시 수신자={_p2pSender.MaxConcurrentReceivers}명");
        }

        private async void BtnSendChat_Click(object sender, RoutedEventArgs e)
        {
            await SendCurrentChatInputAsync();
        }

        private async void TxtChatInput_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Enter)
            {
                await SendCurrentChatInputAsync();
            }
        }

        private async Task SendCurrentChatInputAsync()
        {
            string msg = TxtChatInput.Text.Trim();
            if (string.IsNullOrEmpty(msg)) return;

            if (!(LstPeers.SelectedItem is PeerNodeViewModel selected)) return;

            try
            {
                if (selected.IsSender)
                {
                    await _chatServer.BroadcastChatTextAsync(msg);
                }
                else
                {
                    if (!selected.IsConnected)
                    {
                        MessageBox.Show("먼저 '채팅 시작(접속)' 버튼을 눌러 서버에 연결하세요.", "안내", MessageBoxButton.OK, MessageBoxImage.Information);
                        return;
                    }
                    await selected.ChatClient.SendChatTextAsync(msg);
                }
                TxtChatInput.Clear();
            }
            catch (Exception ex)
            {
                MessageBox.Show($"메시지 전송 실패: {ex.Message}", "오류", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        /// <summary>
        /// 송신자(A)가 파일을 선택하면:
        /// 1) P2PSenderNode에 파일 등록(CreateTransferOffer)
        /// 2) 채팅으로는 오직 "파일명/크기 목록"이 있다는 알림(BroadcastFileNoticeAsync)만 전송
        /// </summary>
        private async void BtnSelectAndSendFiles_Click(object sender, RoutedEventArgs e)
        {
            var dlg = new OpenFileDialog
            {
                Title = "P2P로 전송할 파일 선택 (복수 선택 가능 - 순차 전송됨)",
                Multiselect = true
            };

            if (dlg.ShowDialog() == true && dlg.FileNames.Length > 0)
            {
                await RegisterAndAnnounceFilesAsync(dlg.FileNames, null);
            }
        }

        private async void BtnSendSampleFiles_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                string sampleDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "SampleSourceFiles");
                if (!Directory.Exists(sampleDir)) Directory.CreateDirectory(sampleDir);

                string file1 = Path.Combine(sampleDir, "LargeData_Part1.bin");
                string file2 = Path.Combine(sampleDir, "LargeData_Part2.bin");

                CreateDummyFileIfNotExists(file1, 3 * 1024 * 1024);
                CreateDummyFileIfNotExists(file2, 2 * 1024 * 1024);

                await RegisterAndAnnounceFilesAsync(new[] { file1, file2 }, "[샘플 다중 파일 알림] 2개 파일이 준비되었습니다.");
            }
            catch (Exception ex)
            {
                MessageBox.Show($"샘플 파일 생성/전송 실패: {ex.Message}", "오류", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private async Task RegisterAndAnnounceFilesAsync(IEnumerable<string> filePaths, string customMessage)
        {
            BtnApplyOptions_Click(null, null);

            int p2pPort = int.TryParse(TxtP2PPort.Text, out int pp) ? pp : 9002;
            string serverIp = TxtServerIp.Text.Trim();

            _p2pSender.OpenP2PServer(p2pPort, serverIp);
            var offer = _p2pSender.CreateTransferOffer(filePaths, p2pPort);

            // 현재 채팅에 연결된 수신자 목록을 기준으로 송신 목록 UI 초기 행(수신수락 대기) 생성
            foreach (var peer in _chatServer.GetConnectedPeers())
            {
                foreach (var f in offer.Files)
                {
                    SenderTransfers.Add(new SenderTransferItemViewModel
                    {
                        TransferId = offer.TransferId,
                        FileId = f.FileId,
                        FileName = f.FileName,
                        PeerId = peer.PeerId,
                        PeerName = peer.PeerName,
                        TotalBytes = f.FileSize,
                        TotalChunks = f.TotalChunks,
                        CurrentChunk = 0,
                        Progress = 0,
                        Status = TransferStatus.PendingAccept,
                        StatusMessage = "수신 수락 대기 중 (채팅 공지 발송됨)"
                    });
                }
            }

            // 채팅으로는 오직 파일명/크기 알림만 발송
            await _chatServer.BroadcastFileNoticeAsync(offer, customMessage);
        }

        private void CreateDummyFileIfNotExists(string filePath, int sizeBytes)
        {
            if (File.Exists(filePath) && new FileInfo(filePath).Length == sizeBytes)
                return;

            byte[] buffer = new byte[sizeBytes];
            var rnd = new Random(42);
            rnd.NextBytes(buffer);
            File.WriteAllBytes(filePath, buffer);
        }

        /// <summary>
        /// 수신자가 '수락/이어받기' 클릭 시 채팅을 거치지 않고 곧바로 P2P 포트(P2PReceiverNode)로 접속해 패킷 번호 요청을 시작합니다.
        /// </summary>
        private async void BtnAcceptSingleItem_Click(object sender, RoutedEventArgs e)
        {
            if (!((sender as Button)?.Tag is ReceiverTransferItemViewModel item)) return;

            var ownerNode = ReceiverNodesOnly.FirstOrDefault(x => x.PeerId == item.OwnerPeerId);
            if (ownerNode == null) return;

            string serverIp = TxtServerIp.Text.Trim();
            int p2pPort = int.TryParse(TxtP2PPort.Text, out int pp) ? pp : 9002;

            // P2P 서버가 켜져 있도록 보장
            if (_senderPeerVm.IsConnected)
            {
                _p2pSender.OpenP2PServer(p2pPort, serverIp);
            }

            await ownerNode.ReceiverInstance.StartOrResumeTransferAsync(item.TransferId, serverIp, p2pPort);
        }

        private async void BtnPauseSingleItem_Click(object sender, RoutedEventArgs e)
        {
            if (!((sender as Button)?.Tag is ReceiverTransferItemViewModel item)) return;

            var ownerNode = ReceiverNodesOnly.FirstOrDefault(x => x.PeerId == item.OwnerPeerId);
            if (ownerNode != null)
            {
                await ownerNode.ReceiverInstance.PauseTransferAsync(item.TransferId);
            }
        }

        private async void BtnCancelSingleItem_Click(object sender, RoutedEventArgs e)
        {
            if (!((sender as Button)?.Tag is ReceiverTransferItemViewModel item)) return;

            var ownerNode = ReceiverNodesOnly.FirstOrDefault(x => x.PeerId == item.OwnerPeerId);
            if (ownerNode != null)
            {
                // 아직 수신 시작(P2P 연결 수립) 전인 PendingAccept 상태이거나 연결 끊김 상태라면 채팅으로도 취소 통보
                if (item.Status == TransferStatus.PendingAccept || item.Status == TransferStatus.Error || item.Status == TransferStatus.SenderOffline)
                {
                    await ownerNode.ChatClient.SendPreCancelNoticeAsync(item.TransferId);
                }

                // P2P 연결이 수립된 상태라면 P2P 제어 명령(P2PCommand.Cancel) 전송 및 임시 파일 정리
                await ownerNode.ReceiverInstance.CancelTransferAsync(item.TransferId);
            }
        }

        private async void BtnReceiverAcceptAll_Click(object sender, RoutedEventArgs e)
        {
            if (!(CmbReceiverFilter.SelectedItem is PeerNodeViewModel selectedReceiver)) return;

            string serverIp = TxtServerIp.Text.Trim();
            int p2pPort = int.TryParse(TxtP2PPort.Text, out int pp) ? pp : 9002;

            var transferIds = selectedReceiver.ReceivedFiles
                .Where(x => x.CanAccept)
                .Select(x => x.TransferId)
                .Distinct()
                .ToList();

            foreach (var tid in transferIds)
            {
                await selectedReceiver.ReceiverInstance.StartOrResumeTransferAsync(tid, serverIp, p2pPort);
            }
        }

        private async void BtnAllClientsAcceptSimultaneously_Click(object sender, RoutedEventArgs e)
        {
            string serverIp = TxtServerIp.Text.Trim();
            int p2pPort = int.TryParse(TxtP2PPort.Text, out int pp) ? pp : 9002;

            var tasks = new List<Task>();
            foreach (var rNode in ReceiverNodesOnly)
            {
                var transferIds = rNode.ReceivedFiles
                    .Where(x => x.CanAccept)
                    .Select(x => x.TransferId)
                    .Distinct()
                    .ToList();

                foreach (var tid in transferIds)
                {
                    tasks.Add(rNode.ReceiverInstance.StartOrResumeTransferAsync(tid, serverIp, p2pPort));
                }
            }
            await Task.WhenAll(tasks);
        }

        private async void BtnSenderPauseSelected_Click(object sender, RoutedEventArgs e)
        {
            if (GridSenderTransfers.SelectedItem is SenderTransferItemViewModel selected)
            {
                await _p2pSender.PauseTransferAsync(selected.TransferId, selected.PeerId);
            }
        }

        private async void BtnSenderResumeSelected_Click(object sender, RoutedEventArgs e)
        {
            if (GridSenderTransfers.SelectedItem is SenderTransferItemViewModel selected)
            {
                await _p2pSender.ResumeTransferAsync(selected.TransferId, selected.PeerId);
            }
        }

        private async void BtnSenderCancelSelected_Click(object sender, RoutedEventArgs e)
        {
            if (GridSenderTransfers.SelectedItem is SenderTransferItemViewModel selected)
            {
                // 아직 수신자가 수신 시작(P2P 연결 수립)을 누르기 전이면 채팅으로 취소 알림 전송
                if (selected.Status == TransferStatus.PendingAccept || selected.Status == TransferStatus.Error || selected.Status == TransferStatus.SenderOffline)
                {
                    await _chatServer.SendPreCancelNoticeAsync(selected.TransferId, selected.PeerId);
                }

                // P2P 엔진에서도 취소 처리 (이미 연결된 경우 P2P 제어 명령 P2PCommand.Cancel 전송)
                await _p2pSender.CancelTransferAsync(selected.TransferId, selected.PeerId, selected.PeerName);
            }
        }

        private async void BtnSenderCancelAll_Click(object sender, RoutedEventArgs e)
        {
            var transferIds = SenderTransfers.Select(x => x.TransferId).Distinct().ToList();
            foreach (var tid in transferIds)
            {
                // 아직 수신 시작 전인 수신자들을 위해 채팅으로 전체 취소 알림 전송
                await _chatServer.SendPreCancelNoticeAsync(tid, null);

                // P2P 연결된 수신자들에게는 P2P 제어 명령으로 취소 전송 및 미연결 항목도 UI 상태 갱신
                var peersForOffer = SenderTransfers
                    .Where(x => x.TransferId == tid)
                    .GroupBy(x => x.PeerId)
                    .Select(g => g.First())
                    .ToList();

                foreach (var p in peersForOffer)
                {
                    await _p2pSender.CancelTransferAsync(tid, p.PeerId, p.PeerName);
                }
            }
        }

        private void TxtDownloadFolder_TextChanged(object sender, TextChangedEventArgs e)
        {
            if (CmbReceiverFilter.SelectedItem is PeerNodeViewModel selectedReceiver)
            {
                selectedReceiver.DownloadFolder = TxtDownloadFolder.Text.Trim();
            }
        }

        private void BtnBrowseDownloadFolder_Click(object sender, RoutedEventArgs e)
        {
            if (!(CmbReceiverFilter.SelectedItem is PeerNodeViewModel selectedReceiver)) return;

            using (var dlg = new System.Windows.Forms.FolderBrowserDialog())
            {
                dlg.Description = $"{selectedReceiver.DisplayName}의 다운로드 폴더 선택";
                dlg.SelectedPath = selectedReceiver.DownloadFolder;
                if (dlg.ShowDialog() == System.Windows.Forms.DialogResult.OK)
                {
                    selectedReceiver.DownloadFolder = dlg.SelectedPath;
                    TxtDownloadFolder.Text = dlg.SelectedPath;
                }
            }
        }

        private void BtnOpenDownloadFolder_Click(object sender, RoutedEventArgs e)
        {
            string folder = TxtDownloadFolder.Text.Trim();
            if (!string.IsNullOrEmpty(folder))
            {
                if (!Directory.Exists(folder)) Directory.CreateDirectory(folder);
                Process.Start("explorer.exe", folder);
            }
        }

        private void BtnCheckMissingFiles_Click(object sender, RoutedEventArgs e)
        {
            foreach (var rNode in ReceiverNodesOnly)
            {
                rNode.ReceiverInstance?.CheckFilesAndExpirationStatus();
            }
        }

        private void BtnDeleteCompletedFileTest_Click(object sender, RoutedEventArgs e)
        {
            if (GridReceiverTransfers.SelectedItem is ReceiverTransferItemViewModel item)
            {
                if (!string.IsNullOrEmpty(item.SavedFilePath) && File.Exists(item.SavedFilePath))
                {
                    File.Delete(item.SavedFilePath);
                    BtnCheckMissingFiles_Click(null, null);
                }
                else
                {
                    MessageBox.Show("먼저 수신 완료된 파일 항목을 선택해주세요.", "안내", MessageBoxButton.OK, MessageBoxImage.Information);
                }
            }
            else
            {
                MessageBox.Show("수신 파일 목록에서 삭제 테스트할 완료된 항목을 선택해주세요.", "안내", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }

        private void Window_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            _uiTimer?.Stop();
            foreach (var r in ReceiverNodesOnly)
            {
                r.ChatClient?.Dispose();
                r.ReceiverInstance?.Dispose();
            }
            _p2pSender?.Dispose();
            _chatServer?.Dispose();
        }
    }
}
