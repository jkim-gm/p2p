using System;
using System.Collections.Generic;

namespace P2PTransferLib
{
    /// <summary>
    /// P2P 프로토콜 프레임 헤더의 명령/패킷 타입 (1 Byte)
    /// 헤더에 제어명령인지 데이터 패킷인지 구분하여 단일 P2P 포트에서 모든 제어 및 데이터 전송을 처리합니다.
    /// </summary>
    public enum P2PCommand : byte
    {
        // --- 세션 및 제어 명령 (Control Commands) ---
        Handshake = 0x01,       // 수신자 -> 송신자: 세션 연결 (PeerId, PeerName, TransferId, FileId)
        RequestChunk = 0x02,    // 수신자 -> 송신자: 특정 조각 번호(ChunkIndex) 요청 (패킷 크기가 일정하므로 진행률 산출 기준이 됨)
        WaitQueue = 0x03,       // 송신자 -> 수신자: 동시 전송 제한으로 인한 수신 대기열(WaitingQueue) 상태 알림
        QueueReady = 0x04,      // 송신자 -> 수신자: 대기열 통과, 조각 요청 시작 가능 알림
        Pause = 0x05,           // 양방향: 전송 일시정지 명령
        Resume = 0x06,          // 양방향: 전송 재개(이어받기) 명령
        Cancel = 0x07,          // 양방향: 전송 취소 명령
        FileCompleted = 0x08,   // 수신자 -> 송신자: 모든 조각 수신 및 병합 완료 통보
        Error = 0x09,           // 송신자 -> 수신자: 만료(Expired), 파일없음 등 오류 통보
        Disconnect = 0x0A,      // 송신자 -> 수신자: 송신자 서버 종료/네트워크 단절 알림

        // --- 데이터 패킷 (Data Packet) ---
        DataChunk = 0x20        // 송신자 -> 수신자: 암호화된 파일 조각 데이터 + SHA-256 무결성 해시
    }

    /// <summary>
    /// 파일 전송 상태 열거형
    /// </summary>
    public enum TransferStatus
    {
        PendingAccept,    // 수신 수락 대기 중 (30분 유효시간)
        Expired,          // 유효시간(30분) 만료됨
        WaitingQueue,     // 동시 전송 제한으로 인한 수신 대기 중
        Transferring,     // 조각 전송 중
        Paused,           // 일시정지됨 (이어받기 가능)
        Merging,          // 수신 완료 후 무결성 검증 및 파일 병합 중
        Completed,        // 전송 완료
        Cancelled,        // 사용자에 의해 취소됨
        Error,            // 네트워크 오류 또는 무결성 오류 (재접속 시 이어받기 가능)
        SenderOffline,    // 송신자 네트워크 끊김
        FileMissing       // 다운로드 완료 후 로컬 디스크에서 파일이 삭제/이동됨
    }

    /// <summary>
    /// 전송할 개별 파일 메타데이터 (채팅으로 최초 알림 시에는 파일명/크기/ID만 전달 가능)
    /// </summary>
    [Serializable]
    public class FileItemMeta
    {
        public string FileId { get; set; }
        public string FileName { get; set; }
        public long FileSize { get; set; }
        public int ChunkSize { get; set; }
        public int TotalChunks { get; set; }

        /// <summary>
        /// 로컬 원본 파일 전체 경로 (송신 측에서만 사용)
        /// </summary>
        public string LocalSourcePath { get; set; }
    }

    /// <summary>
    /// 파일 전송 세션 메타데이터
    /// </summary>
    [Serializable]
    public class TransferOffer
    {
        public string TransferId { get; set; }
        public string SenderName { get; set; }
        public string SenderIp { get; set; }
        public int P2PPort { get; set; }
        public int ChunkSize { get; set; } = 64 * 1024;
        public long CreatedUtcTicks { get; set; }
        public long ExpireUtcTicks { get; set; }
        public List<FileItemMeta> Files { get; set; } = new List<FileItemMeta>();

        public DateTime CreatedTimeUtc => new DateTime(CreatedUtcTicks, DateTimeKind.Utc);
        public DateTime ExpireTimeUtc => new DateTime(ExpireUtcTicks, DateTimeKind.Utc);

        public bool IsExpired => DateTime.UtcNow > ExpireTimeUtc;

        public TimeSpan RemainingValidTime
        {
            get
            {
                var rem = ExpireTimeUtc - DateTime.UtcNow;
                return rem.TotalSeconds > 0 ? rem : TimeSpan.Zero;
            }
        }
    }

    /// <summary>
    /// 송신자/수신자 전송 진행 상태 이벤트 인자
    /// (수신자가 요청한 조각 번호와 고정 패킷 크기 ChunkSize를 기반으로 전송 상태를 계산)
    /// </summary>
    public class TransferProgressEventArgs : EventArgs
    {
        public string TransferId { get; set; }
        public string FileId { get; set; }
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
        public string Message { get; set; }
        public string SavedFilePath { get; set; }
    }
}
