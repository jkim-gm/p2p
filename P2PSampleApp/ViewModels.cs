using System;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Runtime.CompilerServices;
using P2PTransferLib;

namespace P2PSampleApp
{
    public class NotifyBase : INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;
        protected void OnPropertyChanged([CallerMemberName] string propName = null)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(propName));
        }
    }

    /// <summary>
    /// 송신자(A)의 파일 송신 목록 아이템 (수신자가 요청한 패킷 번호 기반으로 P2P 엔진이 상태 업데이트)
    /// </summary>
    public class SenderTransferItemViewModel : NotifyBase
    {
        public string TransferId { get; set; }
        public string FileId { get; set; }
        public string FileName { get; set; }
        public string PeerId { get; set; }
        public string PeerName { get; set; }
        public long TotalBytes { get; set; }
        public string FileSizeDisplay => P2PSenderNode.FormatBytes(TotalBytes);

        private double _progress;
        public double Progress
        {
            get => _progress;
            set { _progress = value; OnPropertyChanged(); OnPropertyChanged(nameof(ProgressDisplay)); }
        }

        public string ProgressDisplay => $"{Progress:0.0}% ({CurrentChunk}/{TotalChunks} 패킷)";

        private int _currentChunk;
        public int CurrentChunk
        {
            get => _currentChunk;
            set { _currentChunk = value; OnPropertyChanged(); OnPropertyChanged(nameof(ProgressDisplay)); }
        }

        private int _totalChunks;
        public int TotalChunks
        {
            get => _totalChunks;
            set { _totalChunks = value; OnPropertyChanged(); OnPropertyChanged(nameof(ProgressDisplay)); }
        }

        private TransferStatus _status;
        public TransferStatus Status
        {
            get => _status;
            set { _status = value; OnPropertyChanged(); OnPropertyChanged(nameof(StatusKor)); }
        }

        private string _statusMessage;
        public string StatusMessage
        {
            get => _statusMessage;
            set { _statusMessage = value; OnPropertyChanged(); }
        }

        public string StatusKor => TransferStatusHelper.ToKorean(Status);
    }

    /// <summary>
    /// 수신자(B, C, D, E, F...)의 파일 수신 목록 아이템
    /// </summary>
    public class ReceiverTransferItemViewModel : NotifyBase
    {
        public string OwnerPeerId { get; set; }
        public string OwnerPeerName { get; set; }
        public string TransferId { get; set; }
        public string FileId { get; set; }
        public string FileName { get; set; }
        public long TotalBytes { get; set; }
        public int P2PPort { get; set; }
        public string FileSizeDisplay => P2PSenderNode.FormatBytes(TotalBytes);
        public DateTime ExpireTimeUtc { get; set; }

        private double _progress;
        public double Progress
        {
            get => _progress;
            set { _progress = value; OnPropertyChanged(); OnPropertyChanged(nameof(ProgressDisplay)); }
        }

        private int _currentChunk;
        public int CurrentChunk
        {
            get => _currentChunk;
            set { _currentChunk = value; OnPropertyChanged(); OnPropertyChanged(nameof(ProgressDisplay)); }
        }

        private int _totalChunks;
        public int TotalChunks
        {
            get => _totalChunks;
            set { _totalChunks = value; OnPropertyChanged(); OnPropertyChanged(nameof(ProgressDisplay)); }
        }

        public string ProgressDisplay => $"{Progress:0.0}% ({CurrentChunk}/{TotalChunks} 패킷)";

        private TransferStatus _status;
        public TransferStatus Status
        {
            get => _status;
            set
            {
                _status = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(StatusKor));
                OnPropertyChanged(nameof(CanAccept));
                OnPropertyChanged(nameof(CanPause));
                OnPropertyChanged(nameof(CanCancel));
            }
        }

        private string _statusMessage;
        public string StatusMessage
        {
            get => _statusMessage;
            set { _statusMessage = value; OnPropertyChanged(); }
        }

        private string _remainingValidTimeText;
        public string RemainingValidTimeText
        {
            get => _remainingValidTimeText;
            set { _remainingValidTimeText = value; OnPropertyChanged(); }
        }

        private string _savedFilePath;
        public string SavedFilePath
        {
            get => _savedFilePath;
            set { _savedFilePath = value; OnPropertyChanged(); }
        }

        public string StatusKor => TransferStatusHelper.ToKorean(Status);

        public bool CanAccept => Status == TransferStatus.PendingAccept || Status == TransferStatus.Paused || Status == TransferStatus.Error || Status == TransferStatus.SenderOffline;
        public bool CanPause => Status == TransferStatus.Transferring;
        public bool CanCancel => Status == TransferStatus.PendingAccept || Status == TransferStatus.WaitingQueue || Status == TransferStatus.Transferring || Status == TransferStatus.Paused || Status == TransferStatus.Error || Status == TransferStatus.SenderOffline;

        public void UpdateRemainingTime()
        {
            if (Status == TransferStatus.Completed || Status == TransferStatus.Cancelled || Status == TransferStatus.FileMissing)
            {
                RemainingValidTimeText = "-";
                return;
            }

            var rem = ExpireTimeUtc - DateTime.UtcNow;
            if (rem.TotalSeconds <= 0)
            {
                RemainingValidTimeText = "00:00 (만료됨)";
                if (Status == TransferStatus.PendingAccept)
                {
                    Status = TransferStatus.Expired;
                    StatusMessage = "유효시간(30분) 만료됨";
                }
            }
            else
            {
                RemainingValidTimeText = $"{(int)rem.TotalMinutes:D2}:{rem.Seconds:D2}";
            }
        }
    }

    /// <summary>
    /// 좌측 채팅 목록의 노드(A, B, C, D, E, F...) 뷰모델
    /// (SampleChatClient + P2PReceiverNode 보유)
    /// </summary>
    public class PeerNodeViewModel : NotifyBase
    {
        public string PeerId { get; set; }
        public string DisplayName { get; set; }
        public bool IsSender { get; set; }

        public SampleChatClient ChatClient { get; set; }
        public P2PReceiverNode ReceiverInstance { get; set; }

        private bool _isConnected;
        public bool IsConnected
        {
            get => _isConnected;
            set
            {
                _isConnected = value;
                OnPropertyChanged();
                OnPropertyChanged(nameof(ConnectionStateText));
                OnPropertyChanged(nameof(ConnectButtonText));
            }
        }

        public string ConnectionStateText => IsSender
            ? (IsConnected ? "채팅/P2P 서버 가동중" : "서버 중지됨(끊김)")
            : (IsConnected ? "채팅 접속됨 (Online)" : "미접속 (Offline)");

        public string ConnectButtonText => IsSender
            ? (IsConnected ? "서버 중지(끊김테스트)" : "서버 재시작")
            : (IsConnected ? "연결 끊기" : "채팅 시작(접속)");

        private string _downloadFolder;
        public string DownloadFolder
        {
            get => _downloadFolder;
            set
            {
                _downloadFolder = value;
                if (ReceiverInstance != null)
                {
                    ReceiverInstance.DownloadFolder = value;
                }
                OnPropertyChanged();
            }
        }

        public ObservableCollection<string> ChatLogs { get; } = new ObservableCollection<string>();
        public ObservableCollection<ReceiverTransferItemViewModel> ReceivedFiles { get; } = new ObservableCollection<ReceiverTransferItemViewModel>();
    }

    public static class TransferStatusHelper
    {
        public static string ToKorean(TransferStatus status)
        {
            switch (status)
            {
                case TransferStatus.PendingAccept: return "수신수락 대기";
                case TransferStatus.Expired: return "유효시간 만료";
                case TransferStatus.WaitingQueue: return "수신 대기중";
                case TransferStatus.Transferring: return "전송중";
                case TransferStatus.Paused: return "일시정지됨";
                case TransferStatus.Merging: return "병합/검증중";
                case TransferStatus.Completed: return "전송완료";
                case TransferStatus.Cancelled: return "취소됨";
                case TransferStatus.Error: return "오류(이어받기대기)";
                case TransferStatus.SenderOffline: return "송신자 끊김";
                case TransferStatus.FileMissing: return "파일없음";
                default: return status.ToString();
            }
        }
    }
}
