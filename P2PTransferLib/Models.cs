using System;

namespace P2PTransferLib
{
    /// <summary>
    /// P2P 프로토콜 프레임 헤더의 명령/패킷 타입 (1 Byte)
    /// </summary>
    public enum P2PCommand : byte
    {
        Handshake = 0x01,       // 수신자 -> 송신자: peerId(사번), peerName(이름), chId(채널ID), msgId(파일ID) 전송
        HandshakeAck = 0x02,    // 송신자 -> 수신자: 등록된 파일 정보(파일명, 크기, 총패킷수, 패킷크기, 유효시간 ExpireUtcTicks) 응답
        WaitQueue = 0x03,       // 송신자 -> 수신자: 동시 전송 제한 초과로 인한 대기 상태 알림
        QueueReady = 0x04,      // 송신자 -> 수신자: 대기열 통과, 패킷 요청 시작 가능 알림
        RequestChunk = 0x05,    // 수신자 -> 송신자: 패킷 번호(ChunkIndex) 1개 요청
        DataChunk = 0x20,       // 송신자 -> 수신자: 암호화된 파일 조각 데이터(64KB) + SHA-256 해시 전송
        Pause = 0x06,           // 양방향: 일시정지 제어 명령
        Resume = 0x07,          // 양방향: 전송 재개 제어 명령
        Cancel = 0x08,          // 양방향: 전송 취소 제어 명령
        FileCompleted = 0x09,   // 수신자 -> 송신자: 모든 패킷 수신 및 파일 병합 완료 통보
        Error = 0x0A,           // 송신자 -> 수신자: 미등록 chId/msgId, 유효시간 만료, 파일 없음 등 오류 통보
        QueryMetaOnly = 0x0B    // 수신자 -> 송신자: 다운로드 시작 전 파일명/크기 메타데이터만 조회 (이어받기 퍼센트 사전 확인용)
    }

    /// <summary>
    /// 파일 전송 상태
    /// </summary>
    public enum TransferStatus
    {
        Ready,            // 송신 파일 등록됨 (수신 요청 대기 중)
        WaitingQueue,     // 동시 수신 제한 대기 중
        Transferring,     // 패킷 전송 중
        Paused,           // 일시정지됨
        Merging,          // 파일 병합 중
        Completed,        // 전송 완료
        Cancelled,        // 전송 취소됨
        Expired,          // 유효시간(30분) 만료됨
        Error,            // 오류 발생 (재요청 시 이어받기 가능)
        FileMissing       // 완료 후 로컬 디스크에서 파일이 삭제/이동됨 (파일없음)
    }

    /// <summary>
    /// 송신/수신 시 발생할 수 있는 상세 오류 코드
    /// </summary>
    public enum P2PErrorCode
    {
        None = 0,
        InvalidParameter,           // 잘못된 입력값 (빈 경로, 잘못된 포트/IP, 빈 msgID 등)
        SourceFileNotFound,         // RegisterFile 시 파일이 없거나 원본 파일이 삭제됨
        UnregisteredFileRequested,  // RegisterFile 하지 않은 chID/msgID를 수신자가 요청함
        FileExpired,                // 30분 유효시간이 만료된 파일을 요청함
        ServerBindError,            // 서버 포트 오픈 실패 (이미 사용 중인 포트 또는 잘못된 바인딩 IP)
        ConnectionFailed,           // 서버에 네트워크 접속 실패 (서버 미가동, IP/포트 오류, 타임아웃 등)
        NetworkDisconnected,        // 전송 도중 네트워크 연결 끊김
        IntegrityCheckFailed,       // 패킷 SHA-256 무결성 검증 실패
        DiskAccessError,            // 폴더 생성 실패, 파일 접근 권한/잠금 오류, 디스크 공간 부족
        ProtocolError               // 패킷 스타트 코드 불일치 또는 알 수 없는 명령
    }

    /// <summary>
    /// 오류 발생 시 전달되는 상세 오류 이벤트 인자
    /// </summary>
    public class P2PErrorEventArgs : EventArgs
    {
        public P2PErrorCode ErrorCode { get; set; }
        public string ChId { get; set; }
        public string MsgId { get; set; }
        public string PeerId { get; set; }
        public string PeerName { get; set; }
        public string ErrorMessage { get; set; }
        public Exception Exception { get; set; }
    }

    /// <summary>
    /// 기존에 받다가 중지된 파일(.part)의 사전 진행률 확인 결과 클래스
    /// </summary>
    public class PartialDownloadCheckResult
    {
        public string ChId { get; set; }
        public string MsgId { get; set; }
        public string FileName { get; set; }
        public string PartFilePath { get; set; }
        public string FinalFilePath { get; set; }
        public bool HasPartialFile { get; set; }
        public bool IsAlreadyCompleted { get; set; }
        public long SavedBytes { get; set; }
        public long TotalBytes { get; set; }
        public int SavedChunkCount { get; set; }
        public int TotalChunks { get; set; }
        public double SavedPercentage { get; set; }
        public string SummaryMessage { get; set; }
    }

    /// <summary>
    /// 송신자(서버)에 등록된 파일 정보 (chID + msgID 기준, 상태 및 30분 유효시간 확인 가능)
    /// </summary>
    [Serializable]
    public class RegisteredFileItem
    {
        public string ChId { get; set; }        // 채널ID (chID)
        public string MsgId { get; set; }       // 파일/메시지ID (msgID)
        public string FileKey => $"{ChId}_{MsgId}";
        public string FileName { get; set; }
        public string LocalSourcePath { get; set; }
        public long FileSize { get; set; }
        public int ChunkSize { get; set; }
        public int TotalChunks { get; set; }
        public long CreatedUtcTicks { get; set; }
        public long ExpireUtcTicks { get; set; }

        /// <summary>
        /// Watch 기능에 의해 파일 삭제가 이미 감지되어 알림이 발생했는지 여부
        /// </summary>
        public bool IsMissingNotified { get; set; }

        /// <summary>
        /// Watch 기능에 의해 유효시간 만료 알림이 이미 발생했는지 여부
        /// </summary>
        public bool IsExpiredNotified { get; set; }

        public DateTime CreatedTimeUtc => new DateTime(CreatedUtcTicks, DateTimeKind.Utc);
        public DateTime ExpireTimeUtc => new DateTime(ExpireUtcTicks, DateTimeKind.Utc);

        /// <summary>
        /// 유효시간(30분) 만료 여부
        /// </summary>
        public bool IsExpired => DateTime.UtcNow > ExpireTimeUtc;

        /// <summary>
        /// 원본 파일 존재 여부
        /// </summary>
        public bool IsSourceFileExists => !string.IsNullOrEmpty(LocalSourcePath) && System.IO.File.Exists(LocalSourcePath);

        /// <summary>
        /// 현재 등록된 송신 파일의 상태 (Ready, Expired, FileMissing)
        /// </summary>
        public TransferStatus Status
        {
            get
            {
                if (!IsSourceFileExists) return TransferStatus.FileMissing;
                if (IsExpired) return TransferStatus.Expired;
                return TransferStatus.Ready;
            }
        }

        /// <summary>
        /// 남은 유효시간 (TimeSpan)
        /// </summary>
        public TimeSpan RemainingValidTime
        {
            get
            {
                var rem = ExpireTimeUtc - DateTime.UtcNow;
                return rem.TotalSeconds > 0 ? rem : TimeSpan.Zero;
            }
        }

        /// <summary>
        /// 남은 유효시간 표시 문자열 (예: "29:45 남음" 또는 "00:00 (유효시간 만료)")
        /// </summary>
        public string RemainingValidTimeText
        {
            get
            {
                if (IsExpired) return "00:00 (유효시간 만료)";
                var rem = RemainingValidTime;
                return $"{(int)rem.TotalMinutes:D2}:{rem.Seconds:D2} 남음";
            }
        }
    }

    /// <summary>
    /// 송신자(서버)에서 각 수신자별로 전송 중이거나 완료된 송신 상태 항목
    /// </summary>
    public class SenderTransferStatusItem
    {
        public string SessionKey { get; set; }
        public string ChId { get; set; }
        public string MsgId { get; set; }
        public string FileName { get; set; }
        public long FileSize { get; set; }
        public string PeerId { get; set; }
        public string PeerName { get; set; }
        public int CurrentChunkIndex { get; set; }
        public int TotalChunks { get; set; }
        public double ProgressPercentage => TotalChunks > 0 ? Math.Min(100.0, CurrentChunkIndex * 100.0 / TotalChunks) : 0.0;
        public TransferStatus Status { get; set; }
        public DateTime ExpireTimeUtc { get; set; }
        public bool IsExpired => DateTime.UtcNow > ExpireTimeUtc;

        public TimeSpan RemainingValidTime
        {
            get
            {
                var rem = ExpireTimeUtc - DateTime.UtcNow;
                return rem.TotalSeconds > 0 ? rem : TimeSpan.Zero;
            }
        }

        public string RemainingValidTimeText
        {
            get
            {
                if (Status == TransferStatus.Completed) return "전송완료";
                if (Status == TransferStatus.Cancelled) return "취소됨";
                if (IsExpired) return "00:00 (만료됨)";
                var rem = RemainingValidTime;
                return $"{(int)rem.TotalMinutes:D2}:{rem.Seconds:D2} 남음";
            }
        }
    }

    /// <summary>
    /// 수신자가 수신 시작 시 송신자에게 보내는 요청 헤더 정보 (사번, 이름, 채널ID, 파일ID)
    /// </summary>
    [Serializable]
    public class HandshakeRequest
    {
        public string PeerId { get; set; }      // 사번
        public string PeerName { get; set; }    // 이름
        public string ChId { get; set; }        // 채널ID (chID)
        public string MsgId { get; set; }       // 파일ID (msgID)
    }

    /// <summary>
    /// 송신자가 수신자에게 응답하는 파일 메타데이터 (유효시간 ExpireUtcTicks 포함)
    /// </summary>
    [Serializable]
    public class HandshakeResponse
    {
        public string ChId { get; set; }
        public string MsgId { get; set; }
        public string FileName { get; set; }
        public long FileSize { get; set; }
        public int ChunkSize { get; set; }
        public int TotalChunks { get; set; }
        public long CreatedUtcTicks { get; set; }
        public long ExpireUtcTicks { get; set; }
    }

    /// <summary>
    /// 송신 또는 수신 등록된 파일이 로컬 디스크에서 삭제/이동되었을 때 Watch 기능이 발생시키는 이벤트 인자
    /// </summary>
    public class FileWatchEventArgs : EventArgs
    {
        public string ChId { get; set; }
        public string MsgId { get; set; }
        public string FileName { get; set; }
        public string MissingFilePath { get; set; }
        public string PeerId { get; set; }
        public string PeerName { get; set; }
        public TransferStatus Status { get; set; } // FileMissing 또는 Expired
        public string Message { get; set; }
    }

    /// <summary>
    /// 전송 진행률 및 상태 이벤트 인자
    /// </summary>
    public class TransferProgressEventArgs : EventArgs
    {
        public string SessionKey { get; set; }
        public string ChId { get; set; }
        public string MsgId { get; set; }
        public string FileName { get; set; }
        public string PeerId { get; set; }
        public string PeerName { get; set; }
        public int CurrentChunkIndex { get; set; }
        public int TotalChunks { get; set; }
        public int ChunkSize { get; set; }
        public long TransferredBytes { get; set; }
        public long TotalBytes { get; set; }
        public double ProgressPercentage => TotalBytes > 0 ? Math.Min(100.0, (double)TransferredBytes * 100.0 / TotalBytes) : 100.0;
        public TransferStatus Status { get; set; }
        public P2PErrorCode ErrorCode { get; set; }
        public DateTime ExpireTimeUtc { get; set; }
        public TimeSpan RemainingValidTime
        {
            get
            {
                if (ExpireTimeUtc == DateTime.MinValue) return TimeSpan.Zero;
                var rem = ExpireTimeUtc - DateTime.UtcNow;
                return rem.TotalSeconds > 0 ? rem : TimeSpan.Zero;
            }
        }
        public string Message { get; set; }
        public string SavedFilePath { get; set; }
    }
}
