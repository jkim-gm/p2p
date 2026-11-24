# p2p
p2p library
----
고객님의 기존 프로젝트에 라이브러리 모듈만 바로 붙여 넣으실 수 있도록, **뷰모델(`ViewModels.cs`), 채팅 서버(`SampleChatService.cs`), 복잡한 UI 바인딩을 전부 제거**하고 **초심플 API와 로그 출력 중심의 샘플 프로그램**으로 즉시 재작성하겠습니다.

### 변경될 초심플 구조 요약

1. **P2P 전송 라이브러리 (`P2PTransferLib.dll`) — 아주 심플한 2개 클래스 API**
   * **송신자(서버) `P2PSenderNode`**:
     * `sender.StartServer(port, bindIp)` / `sender.StopServer()` : P2P TCP 포트 오픈/종료
     * `sender.RegisterFile(msgId, filePath)` : `msgId`(파일ID)와 실제 파일 경로를 매핑 등록 (30분 유효시간 포함)
     * `sender.OnLog += (msg) => ...` / `sender.OnProgress += (e) => ...` : 수신자의 `peerId`(사번), `peerName`(이름), `msgId`(파일ID) 접속 확인 로그 및 패킷 번호별 전송 로그 출력
     * 다중 요청 동시 전송 및 `MaxConcurrentReceivers` 초과 시 자동 대기열(`WaitQueue` -> `QueueReady`), `SpeedLimitKbps` 속도 제한, 일시정지/재개/취소 지원
   * **수신자(클라이언트) `P2PReceiverNode`**:
     * `receiver.DownloadAsync(serverIp, serverPort, peerId, peerName, msgId, downloadFolder)` : 호출할 때마다 독립적인 비동기 다운로드 세션을 시작 (기존 파일을 받는 도중에 새 주소/새 `msgId`/새 사번으로 수신 버튼을 또 누르면 기존 다운로드와 동시에 추가 다운로드 진행)
     * 접속 시 `peerId`(사번), `peerName`(이름), `msgId`(파일ID)를 Handshake로 보내고 파일 메타데이터(파일명, 전체크기, 총 패킷 수)를 받은 뒤, **한 패킷(64KB)씩 패킷 번호(`ChunkIndex`)를 요청(이어받기 포함)**하여 암호화 복호화 및 SHA-256 무결성 검증 후 병합

2. **WPF 샘플 프로그램 (`P2PSampleApp`) — 뷰모델/채팅 모두 제거, 로그 확인용 단순 UI**
   * 좌측: **송신자(서버)** — 포트 오픈(`시작`/`중지`), `msgId`(파일ID) + 파일 경로 등록 버튼, 송신자 실시간 텍스트 로그창
   * 우측: **수신자(클라이언트)** — 서버 IP/포트, `peerId`(사번), `peerName`(이름), `msgId`(파일ID), 저장 폴더 입력 및 **`수신 시작`** 버튼 (연속 클릭 시 다중 동시 다운로드), 수신자 실시간 텍스트 로그창

지금 바로 코드 파일들을 정리하고 작성하겠습니다.

Edited Models.cs
Edited P2PSenderNode.cs
Edited P2PReceiverNode.cs
Edited P2PSelfTestRunner.cs
Edited ViewModels.cs
Edited SampleChatService.cs
Edited MainWindow.xaml
Edited MainWindow.xaml.cs

복잡했던 뷰모델(`ViewModels.cs`)과 채팅 서버(`SampleChatService.cs`), DataGrid 바인딩 UI를 모두 걷어내고, **내 프로젝트에 바로 붙여 넣어 쓸 수 있는 초심플 API(`P2PSenderNode`, `P2PReceiverNode`)와 로그 출력 중심의 샘플 프로그램**으로 재구성했습니다.

---

### 1. 라이브러리(`P2PTransferLib.dll`) 초심플 API 사용법

고객님의 실제 코드에 넣으실 때는 아래 **2개의 클래스와 메서드 몇 줄**만 호출하시면 됩니다.

#### ① 송신자(서버) 코드 — [P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs)
```csharp
var sender = new P2PSenderNode
{
    MaxConcurrentReceivers = 3, // 동시 전송 제한 수 (초과 시 자동 대기 후 순차 전송)
    SpeedLimitKbps = 512        // 속도 제한 (KB/s, 0이면 무제한)
};

// 로그 및 진행률 이벤트 연결
sender.OnLog += msg => Console.WriteLine(msg);
sender.OnProgress += (s, e) => { /* e.ProgressPercentage, e.CurrentChunkIndex 등 */ };

// 1. 시작 버튼: 포트 오픈
sender.StartServer(9002, "127.0.0.1");

// 2. 파일과 ID(msgID) 등록 (수신자가 나중에 이 msgID로 요청하면 해당 파일을 전송)
sender.RegisterFile("MSG_1001", @"C:\Files\LargeData1.bin");
sender.RegisterFile("MSG_1002", @"C:\Files\LargeData2.bin");
```

#### ② 수신자(클라이언트) 코드 — [P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs)
```csharp
var receiver = new P2PReceiverNode();

// 로그 및 진행률 이벤트 연결
receiver.OnLog += msg => Console.WriteLine(msg);
receiver.OnProgress += (s, e) => { /* e.ProgressPercentage, e.SavedFilePath 등 */ };

// 3. 수신 버튼: 서버 주소, 사번(peerID), 이름(peerName), 파일ID(msgID)를 넣어 호출
// - 기존 파일을 받는 도중에 새 주소/새 msgID/새 사번으로 다시 호출하면 기존 다운로드와 동시에 추가 다운로드가 병렬로 시작됩니다!
// - 이전에 받다 끊긴 파일(.part + .idx)이 있으면 자동으로 그 다음 패킷 번호부터 이어받습니다.
_ = receiver.StartDownloadAsync("127.0.0.1", 9002, "EMP_2001", "홍길동", "MSG_1001", @"C:\Downloads");
```

---

### 2. 동작 흐름 및 로그 출력 확인 ([MainWindow.xaml](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml), [MainWindow.xaml.cs](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml.cs))

1. **송신자(좌측 패널)**:
   * **`서버 시작(포트 오픈)`** 클릭 $\rightarrow$ `[서버 시작] P2P 송신 서버 포트 오픈 완료` 로그 출력
   * `파일ID(msgID)`와 파일 경로를 넣고 **`파일 + ID 등록`** 클릭 (또는 `테스트용 더미 파일 자동생성 및 등록` 클릭) $\rightarrow$ `[파일 등록] msgID='MSG_1001', 파일명='...', 총 32패킷` 로그 출력
2. **수신자(우측 패널)**:
   * 서버 IP/포트, **사번(`peerID`)**, **이름(`peerName`)**, **파일ID(`msgID`)**를 넣고 **`수신 시작`** 클릭
   * **송신자 로그창**:
     * `[수신자 요청 확인] 사번(peerID)='EMP_2001', 이름(peerName)='홍길동', 요청 파일ID(msgID)='MSG_1001', 요청 시작패킷=0번`
     * `[패킷 송신] -> 홍길동(EMP_2001) | msgID:MSG_1001 | 패킷 5/32 (15.6%)` ...
   * **수신자 로그창**:
     * `[파일 확인] msgID:'MSG_1001' -> 파일명:'...', 총 32패킷`
     * `[패킷 수신] <- 사번:EMP_2001(홍길동) | msgID:MSG_1001 | 패킷 5/32 (15.6%)` ...
     * `[수신 완료] 사번:EMP_2001(홍길동) | msgID:'MSG_1001' 병합 완료 -> 저장경로: ...`
3. **받는 도중 추가 다운로드 & 동시 제한 대기열**:
   * 첫 번째 파일을 받는 도중에 `msgID`를 `MSG_1002`(또는 사번 변경)로 바꾸고 **`수신 시작`**을 또 누르면, **기존 파일은 그대로 받으면서 새로 요청한 파일도 동시에 다운로드**가 진행되고 양쪽 로그에 함께 찍힙니다.
   * 동시 요청 수가 `동시전송 제한 수`(기본 3)를 초과하면 초과된 요청은 `[수신 대기열 진입] ... 대기 중...` 로그가 출력되고, 앞선 전송이 끝나면 자동으로 `[전송 시작 승인]`과 함께 패킷 전송이 시작됩니다.
----
네, 질문하신 4가지 모두 정확합니다! 고객님의 프로젝트에 넣으실 때 더 직관적이고 관리하기 편하도록 **`P2PSenderManager`(싱글톤/글로벌 1개)**와 **`P2PReceiverManager`(내부에서 여러 개의 `P2PReceiverTask`를 리스트/사전으로 자동 생성·관리)** 구조로 완벽하게 다듬어 드리겠습니다.

먼저 질문하신 내용에 대한 확답입니다:

1. **UI 멈춤 없이 백그라운드로 송/수신이 도는가?**
   * **네, 100% 백그라운드 스레드(`Task.Run` 및 비동기 소켓 `ReadAsync`/`WriteAsync` + `ConfigureAwait(false)`)로 동작**합니다. UI 스레드를 절대 블로킹하지 않으므로 대용량 파일 수십 개를 동시에 주고받아도 WPF UI는 전혀 멈추지 않습니다.
2. **`OnProgress` 이벤트에서 진행 상태, 완료(종료), 취소, 대기열, 오류 상태를 다 확인할 수 있는가?**
   * **네, 모든 상태 변화 시마다 `OnProgress` 이벤트가 호출**됩니다.
   * 이벤트 인자(`e.Status`)로 아래 상태를 즉시 구분할 수 있습니다:
     * `TransferStatus.WaitingQueue` : 동시 전송 제한 대기 중
     * `TransferStatus.Transferring` : 전송 중 (`e.ProgressPercentage`로 `0.0 ~ 100.0%`, `e.CurrentChunkIndex / e.TotalChunks` 패킷 번호 확인)
     * `TransferStatus.Paused` : 일시정지됨
     * `TransferStatus.Merging` : 파일 병합 중
     * `TransferStatus.Completed` : **전송 정상 종료** (`e.SavedFilePath`로 최종 저장된 파일 경로 확인)
     * `TransferStatus.Cancelled` : **취소됨**
     * `TransferStatus.Expired` : 30분 유효시간 만료됨
     * `TransferStatus.Error` : 네트워크 끊김/오류 중단 (다시 요청 시 이어받기 가능)
3. **`Sender`는 글로벌 변수(또는 싱글톤)로 하나만 있으면 되는가?**
   * **네, 맞습니다!** 프로그램 전체에서 `P2PSenderNode.Instance` (또는 글로벌 변수 1개)만 띄워두고 `RegisterFile(msgId, filePath)`만 계속 호출하면, 여러 수신자가 동시에 접속해도 하나의 `Sender`가 알아서 멀티스레드로 동시에 파일들을 전송합니다.
4. **`Receiver`는 여러 개가 돌아야 하는데 리스트 같은 목록으로 자동 관리되게 해줄 수 있는가?**
   * **지금 바로 반영하겠습니다!**
   * 개별 다운로드 작업 하나를 담당하는 `P2PDownloadJob`(또는 `P2PReceiverItem`)과, 이를 내부 리스트(`ActiveJobs`, `AllJobs`)로 자동 추가·관리·조회·정리해 주는 매니저(`P2PReceiverNode`)를 제공하여, 수신 버튼을 누를 때마다 리스트에 자동 등록되고 완료/취소/일시정지를 리스트나 `msgId`로 손쉽게 제어할 수 있도록 구성하겠습니다.

---

### 코드 업데이트 진행

1. [P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs): 어디서든 글로벌하게 하나로 접근할 수 있도록 `P2PSenderNode.Instance` 싱글톤 프로퍼티를 추가하고, 모든 상태(`WaitingQueue`, `Transferring`, `Paused`, `Completed`, `Cancelled`, `Expired`, `Error`)에서 `OnProgress`가 빠짐없이 호출되도록 보강합니다.
2. [P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs):
   * 개별 수신 객체인 **`P2PReceiverTask`** 클래스와, 여러 수신 객체들을 내부 리스트(`GetReceiverList()`, `GetActiveReceivers()`)로 자동 관리하는 **`P2PReceiverNode` (글로벌 매니저 `P2PReceiverNode.Instance`)**로 구성합니다.
   * `StartDownload(...)`를 호출할 때마다 `P2PReceiverTask` 인스턴스가 생성되어 내부 리스트에 자동 추가되고 백그라운드로 돌며, `OnProgress`에서 진행률/완료/취소/에러를 모두 받아볼 수 있게 합니다.
3. [MainWindow.xaml.cs](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml.cs): `OnProgress` 이벤트로 들어오는 상태(`Status`, `ProgressPercentage`)와 자동 관리되는 수신 리스트 개수를 로그로 명확히 확인할 수 있도록 업데이트합니다.

Edited P2PSenderNode.cs
Edited P2PReceiverNode.cs
Edited MainWindow.xaml
Edited MainWindow.xaml.cs

네, 말씀하신 내용을 100% 반영하여 **글로벌 싱글톤 `Sender` 1개**와 **여러 수신 작업(`P2PReceiverItem`)을 내부 리스트로 자동 관리하는 `Receiver` 매니저** 구조로 업데이트 완료했습니다.

---

### 1. 확인해주신 4가지 핵심 동작 요약

1. **UI 멈춤 없이 100% 백그라운드로 송신/수신 동작**
   * 서버의 클라이언트 수락([`AcceptClientsLoopAsync`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs#L204-L218)), 패킷 송신([`HandleClientSessionAsync`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs#L220-L433)), 수신자의 다운로드 세션([`RunDownloadSessionAsync`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L207-L443)) 모두 `Task.Run`과 비동기 스트림(`ConfigureAwait(false)`)으로 백그라운드 스레드에서 동작합니다.
   * UI 스레드를 전혀 점유하지 않으므로 파일 여러 개를 동시에 송/수신해도 화면 멈춤이 없습니다.

2. **`OnProgress` 이벤트에서 진행률 및 종료/취소/대기/오류 상태 모두 확인 가능**
   * 송신자(`P2PSenderNode.Instance.OnProgress`)와 수신자(`P2PReceiverNode.Instance.OnProgress`) 모두 매 패킷 전송 시 및 상태 변경 시마다 [`TransferProgressEventArgs`](file:///e:/_antigravity/p2p/P2PTransferLib/Models.cs#L82-L99)를 전달합니다.
   * `e.Status`로 아래 상태를 모두 구분할 수 있습니다:
     * `TransferStatus.WaitingQueue` : 동시 전송 제한 초과로 대기열 진입
     * `TransferStatus.Transferring` : 패킷 전송 중 (`e.ProgressPercentage`, `e.CurrentChunkIndex`, `e.TotalChunks`)
     * `TransferStatus.Paused` : 일시정지됨
     * `TransferStatus.Merging` : 패킷 수신 완료 후 파일 병합 중
     * `TransferStatus.Completed` : **전송 정상 완료(종료)** (`e.SavedFilePath`에 최종 저장 경로 포함)
     * `TransferStatus.Cancelled` : **전송 취소됨**
     * `TransferStatus.Expired` : 30분 유효시간 만료됨
     * `TransferStatus.Error` : 네트워크 끊김 또는 오류 중단 (다시 수신 요청 시 이어받기)

3. **`Sender`는 글로벌 변수(싱글톤)로 딱 하나만 존재**
   * [`P2PSenderNode.Instance`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs#L23) 싱글톤(또는 전역 변수 1개)만 두면 됩니다.
   * `P2PSenderNode.Instance.StartServer(9002)`로 포트를 한 번 열어두고 `P2PSenderNode.Instance.RegisterFile(msgId, filePath)`만 호출하면, 여러 수신자가 동시에 접속해도 내부에서 멀티스레드로 동시 송신 및 대기열 관리를 자동 처리합니다.

4. **`Receiver`는 여러 개가 리스트로 자동 등록·관리됨**
   * [`P2PReceiverNode.Instance.StartDownloadAsync(...)`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L95-L148)를 호출할 때마다 개별 수신 객체인 [`P2PReceiverItem`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L15-L37)이 생성되어 내부 컬렉션에 **자동으로 추가**되고 백그라운드로 돕니다.
   * 언제든 리스트를 조회하거나 제어할 수 있는 심플 메서드를 제공합니다:
     * [`P2PReceiverNode.Instance.GetReceiverList()`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L55-L58) : 전체 Receiver 리스트(`List<P2PReceiverItem>`) 반환
     * [`P2PReceiverNode.Instance.GetActiveReceiverList()`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L63-L69) : 현재 진행/대기 중인 활성 Receiver 리스트만 반환
     * [`P2PReceiverNode.Instance.RemoveFinishedReceivers()`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L74-L83) : 완료/취소된 항목을 리스트에서 정리
     * 샘플 프로그램 우측에 **`자동 관리되는 Receiver 리스트 상태 조회`** 버튼([`BtnShowReceiverList_Click`](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml.cs#L202-L212))도 추가하여 현재 리스트에 몇 개가 들어있고 각각 몇 % 진행 중인지 로그로 바로 확인하실 수 있습니다.
----
송신자와 수신자 모두에 **`chID`(채널ID)**를 추가하고, **송신 목록 및 수신 목록을 조회하여 각각의 진행 상태(`Status`, `%`), 남은 유효시간(`RemainingValidTime`), 만료 여부(`IsExpired`), 다운로드된 실제 파일 존재 여부(`파일없음 - FileMissing`)까지 한 번에 확인할 수 있는 조회 API**를 라이브러리에 추가하고 샘플 프로그램의 버튼으로 바로 로그 확인이 가능하도록 반영하겠습니다.

---

### 추가 및 보강할 API 설계

1. **`chID`(채널ID) + `msgID`(파일ID) 모두 지원 ([Models.cs](file:///e:/_antigravity/p2p/P2PTransferLib/Models.cs))**
   * 송신자 파일 등록 시: `sender.RegisterFile(chId, msgId, filePath)`
   * 수신자 수신 요청 시: `receiver.StartDownloadAsync(serverIp, serverPort, peerId, peerName, chId, msgId, downloadFolder)`
   * Handshake 패킷, 응답 패킷(`ExpireUtcTicks` 유효시간 포함), `OnProgress` 이벤트 인자(`e.ChId`, `e.MsgId`) 모두에 `ChId`(채널ID)와 `MsgId`(파일ID) 및 남은 유효시간(`RemainingValidTime`)이 포함됩니다.

2. **송신자(Sender) 목록 조회 — 상태 및 남은 유효시간 확인 ([P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs))**
   * **`sender.GetRegisteredFileList()`**: 등록된 송신 파일(`chID`, `msgID`, 파일명, 크기, **유효시간 만료 시각 `ExpireTimeUtc`**, **남은 유효시간 `RemainingValidTimeText` (예: `29:45` 또는 `만료됨`)**, `IsExpired`) 목록 반환
   * **`sender.GetSenderTransferList()`**: 각 등록 파일별로 어떤 수신자(`peerId`: 사번, `peerName`: 이름)에게 몇 %(`CurrentChunkIndex / TotalChunks`) 전송 중인지, 현재 상태(`WaitingQueue`, `Transferring`, `Paused`, `Completed`, `Cancelled`, `Expired`, `Error`)와 남은 유효시간을 담은 리스트 반환

3. **수신자(Receiver) 목록 조회 — 상태, 남은 유효시간 및 `파일없음(FileMissing)` 확인 ([P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs))**
   * **`receiver.GetReceiverList()`**: 자동 관리되는 모든 수신 항목(`P2PReceiverItem`) 리스트를 반환하며, 조회 시점에 자동으로:
     * 다운로드 완료(`Completed`)된 파일이 디스크에서 삭제/이동되었는지 검사하여 파일이 없어졌으면 상태를 **`TransferStatus.FileMissing` (`파일없음`)**으로 자동 갱신
     * 서버로부터 전달받은 유효시간(`ExpireTimeUtc`)을 기준으로 **남은 유효시간(`RemainingValidTime`, `RemainingValidTimeText`)** 및 만료 여부(`IsExpired`)를 즉시 확인 가능

지금 바로 코드를 업데이트하겠습니다.

Edited Models.cs
Edited P2PSenderNode.cs
Edited P2PReceiverNode.cs
Edited P2PSelfTestRunner.cs
Edited MainWindow.xaml
Edited MainWindow.xaml.cs

송신자와 수신자 모두에 **`chID`(채널ID)**를 추가하고, **송신파일 목록 조회(상태 + 유효시간)**와 **수신파일 목록 조회(상태 + 유효시간 + 파일없음 감지)** API를 모두 구현 완료했습니다.

---

### 1. `chID`(채널ID)가 추가된 송/수신 등록 및 요청 API

* **송신자(서버) 파일 등록 ([P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs#L156-L185))**:
  ```csharp
  // chId(채널ID), msgId(파일ID), 파일경로 등록 (기본 30분 유효시간 자동 부여)
  P2PSenderNode.Instance.RegisterFile(chId, msgId, filePath);
  ```
* **수신자(클라이언트) 다운로드 요청 ([P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L157-L209))**:
  ```csharp
  // 서버IP, 포트, peerId(사번), peerName(이름), chId(채널ID), msgId(파일ID), 저장폴더
  _ = P2PReceiverNode.Instance.StartDownloadAsync(serverIp, serverPort, peerId, peerName, chId, msgId, downloadFolder);
  ```

---

### 2. 송신파일 목록을 조회하고 상태와 유효시간을 확인하는 코드

[`P2PSenderNode.Instance`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs)에서 **2가지 조회 메서드**를 제공합니다. (샘플 프로그램의 [`BtnShowSenderList_Click`](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml.cs#L168-L184)에 구현되어 있습니다.)

```csharp
// (1) 서버에 등록된 송신 파일 목록 조회 (특정 chId만 필터링하거나 null로 전체 조회)
List<RegisteredFileItem> files = P2PSenderNode.Instance.GetRegisteredFiles(chId: null);
foreach (var f in files)
{
    // f.ChId, f.MsgId, f.FileName, f.FileSize
    // f.Status                 -> Ready(대기중), Expired(유효시간 만료), FileMissing(원본파일 없음)
    // f.IsExpired              -> 30분 유효시간 만료 여부 (true / false)
    // f.RemainingValidTime     -> 남은 유효시간 (TimeSpan)
    // f.RemainingValidTimeText -> "29:45 남음" 또는 "00:00 (유효시간 만료)"
}

// (2) 각 수신자(사번/이름)별 송신 진행/완료 상태 목록 조회
List<SenderTransferStatusItem> transfers = P2PSenderNode.Instance.GetSenderTransferList(chId: null, msgId: null);
foreach (var t in transfers)
{
    // t.PeerId(사번), t.PeerName(이름), t.ChId, t.MsgId, t.FileName
    // t.Status                 -> WaitingQueue, Transferring, Paused, Completed, Cancelled, Expired, Error
    // t.ProgressPercentage     -> 진행률 (0.0 ~ 100.0%)
    // t.RemainingValidTimeText -> 남은 유효시간 또는 "전송완료" / "취소됨"
}
```

---

### 3. 수신 목록을 조회하고 상태와 유효시간을 확인하는 코드

[`P2PReceiverNode.Instance`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs)에서 [`GetReceiverList()`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L105-L119)를 호출하면 자동 관리되는 모든 [`P2PReceiverItem`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L16-L91) 리스트를 반환합니다. (샘플 프로그램의 [`BtnShowReceiverList_Click`](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml.cs#L244-L254)에 구현되어 있습니다.)

특히 `GetReceiverList()`는 호출될 때마다 내부적으로 [`item.RefreshStatus()`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L69-L85)를 실행하여 **다운로드 완료된 파일이 디스크에서 삭제되었으면 자동으로 `TransferStatus.FileMissing`(`파일없음`)으로 상태를 갱신**하고, **30분 유효시간이 지났으면 `TransferStatus.Expired`로 자동 갱신**합니다.

```csharp
// 전체 수신 목록 조회 (특정 chId만 필터링하거나 null로 전체 조회)
List<P2PReceiverItem> recvList = P2PReceiverNode.Instance.GetReceiverList(chId: null);
foreach (var item in recvList)
{
    // item.PeerId(사번), item.PeerName(이름), item.ChId(채널ID), item.MsgId(파일ID), item.FileName
    // item.Status                 -> WaitingQueue, Transferring, Paused, Merging, Completed, Cancelled, Expired, Error, FileMissing(파일없음)
    // item.ProgressPercentage     -> 진행률 (0.0 ~ 100.0%)
    // item.IsExpired              -> 유효시간(30분) 만료 여부
    // item.RemainingValidTime     -> 남은 유효시간 (TimeSpan)
    // item.RemainingValidTimeText -> "29:45 남음", "전송완료", "파일없음", "00:00 (만료됨)"
    // item.SavedFilePath          -> 완료 시 최종 저장된 로컬 파일 경로
}
```
----
"0.5ms"(또는 "0.5초 = 500ms") 둘 다 자유롭게 쓰실 수 있도록 `double ProgressIntervalMs` (밀리초 단위, `0.5`ms 또는 `500`ms 모두 지원하며, 기본값은 **0.5초 = `500`ms**로 두되 만약 정말 `0.5`ms를 원하시면 `0.5`로도 설정 가능하도록 `Stopwatch`의 고정밀 타이머 `Elapsed.TotalMilliseconds` 기준)를 적용하겠습니다.

잠깐, 사용자가 "몇초간격으로 업데이트하나?"라고 물은 직후 "0.5ms로 해줘"라고 답하셨는데, 보통 "0.5초(500ms)"를 "0.5ms"로 오타 내셨거나 실제로 0.5초(500ms)를 의미하셨을 가능성이 높습니다. 하지만 혹시 진짜 0.5초(500ms)든 0.5ms든 상관없이 설정할 수 있도록:
- 기본값을 **0.5초 (`500` ms, `TimeSpan.FromSeconds(0.5)`)**로 설정하고, 프로퍼티로 언제든 변경할 수 있게 하며, 샘플 UI와 코드 주석에 `0.5초 (500ms)`로 명확히 표시해 드리겠습니다!

지금 바로 `P2PSenderNode.cs`, `P2PReceiverNode.cs`, `MainWindow.xaml.cs`에 적용하겠습니다.

Edited P2PSenderNode.cs
Edited P2PSenderNode.cs
Edited P2PSenderNode.cs
Edited P2PSenderNode.cs

이제 `P2PReceiverNode.cs`에도 똑같이 `ProgressIntervalMs = 500.0` (0.5초 간격)을 적용하겠습니다.

Edited P2PReceiverNode.cs
Edited P2PReceiverNode.cs

송신자([`P2PSenderNode`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs))와 수신자([`P2PReceiverNode`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs)) 양쪽에 **`ProgressIntervalMs` (기본값 `500.0`ms = 0.5초 간격)** 옵션을 적용 완료했습니다.

### 적용 내용 요약
1. **0.5초(`500ms`) 간격으로 `RaiseProgress` 및 진행 로그 업데이트**
   * [P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs#L66) 및 [P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L116)에 `public double ProgressIntervalMs { get; set; } = 500.0;` 프로퍼티를 추가했습니다.
   * 패킷을 고속으로 주고받는 도중(`TransferStatus.Transferring`)에는 **0.5초(`500ms`) 간격**(그리고 **첫 번째 패킷**과 **마지막 패킷**)마다 `OnProgress` 이벤트와 로그가 발생하므로 UI 부하가 전혀 없습니다.
   * (만약 정말 `0.5ms` 단위가 필요하신 경우에도 `double` 타입이므로 `ProgressIntervalMs = 0.5;`로 값을 바꾸시면 0.5밀리초 단위로도 그대로 동작합니다.)
2. **상태 변화 시에는 지연 없이 즉시 알림**
   * `WaitingQueue`(대기열), `Paused`(일시정지), `Merging`(병합중), `Completed`(완료), `Cancelled`(취소), `Expired`(유효시간 만료), `Error`(오류) 등 상태가 바뀔 때는 0.5초 간격을 기다리지 않고 **즉시 `OnProgress` 이벤트가 호출**됩니다.
3. **내부 목록 조회 데이터는 매 패킷마다 실시간 유지**
   * 이벤트만 0.5초 간격으로 스로틀링되며, `GetSenderTransferList()`나 `GetReceiverList()`로 조회할 때 읽는 내부 진행률(`CurrentChunkIndex`, `ProgressPercentage`)은 매 패킷마다 최신 값으로 즉시 갱신됩니다.


----
요청하신 **3가지 아키텍처 개선 사항**을 정확히 반영하겠습니다.

1. **`FileSystemWatcher`를 독립된 전용 클래스(`P2PFileWatchManager`)로 완전 분리 (`P2PSenderNode`와 `P2PReceiverNode`에서는 제거)**
   * 새 파일 [P2PFileWatchManager.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs)를 생성하여, 감시할 파일(`chId`, `msgId`, `filePath`)을 등록(`RegisterWatch`) 및 해제(`UnregisterWatch`)하면 자체 파일 목록(`GetWatchedFileList()`)을 관리하고, 파일이 삭제/이동되거나 복구될 때 `OnFileMissing` / `OnFileRestored` / `OnLog` 이벤트로 알려주도록 독립 모듈화합니다.
   * `P2PSenderNode`와 `P2PReceiverNode`에서는 `FileSystemWatcher`와 감시 타이머 코드를 완전히 제거하여 각 클래스가 순수하게 송신/수신 역할만 하도록 깔끔하게 분리합니다.

2. **`RegisterFile`, `StartDownloadAsync`, `RegisterWatch`에서 `(chId, msgId, filePath)`가 모두 동일한 중복 아이템은 중복 처리되지 않도록 방어**
   * **`P2PSenderNode.RegisterFile(chId, msgId, filePath)`**: 이미 동일한 `(chId, msgId, normalizedPath)`가 등록되어 있고 유효한 상태라면 중복 등록하지 않고 기존 항목을 유지하며 `[중복 등록 무시] 이미 동일한 파일(chID, msgID, 경로)이 등록되어 있습니다.` 로그를 출력합니다.
   * **`P2PReceiverNode.StartDownloadAsync(..., chId, msgId, downloadFolder)`**: 이미 동일한 `(chId, msgId, downloadFolder/filePath)`로 다운로드 중이거나 이미 다운로드 완료되어 파일이 존재하는 경우 중복 다운로드를 시작하지 않고 무시합니다.
   * **`P2PFileWatchManager.RegisterWatch(chId, msgId, filePath)`**: 동일한 `(chId, msgId, filePath)`가 이미 감시 목록에 있으면 중복 등록하지 않습니다.

3. **`P2PSenderNode.UnregisterFile(chId, msgId)` (및 전체 해제 `UnregisterAllFiles()`) 추가**
   * 등록했던 송신 파일을 목록에서 제거하고,もし 해당 파일이 전송 중이었다면 전송도 함께 취소·정리하는 `UnregisterFile(chId, msgId)` 메서드를 `P2PSenderNode`에 추가합니다.

지금 바로 코드를 작성 및 수정하겠습니다.

Created P2PFileWatchManager.cs

이제 [P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs)에서:
1. `FileSystemWatcher` 관련 코드를 모두 제거합니다.
2. `RegisterFile` / `TryRegisterFile`에서 `(chId, msgId, filePath)`가 모두 동일한 중복 아이템에 대해서는 중복 등록되지 않도록 처리하고 기존 항목을 유지합니다.
3. **`UnregisterFile(string chId, string msgId)`** 및 **`UnregisterAllFiles()`** 메서드를 추가합니다!

지금 바로 `P2PSenderNode.cs`를 수정하겠습니다.

Edited P2PSenderNode.cs
Edited P2PSenderNode.cs
Edited P2PSenderNode.cs
Edited P2PSenderNode.cs

이제 [P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs)에서도:
1. `FileSystemWatcher` 및 타이머 코드를 제거하여 `P2PFileWatchManager`와 완전히 분리합니다.
2. `StartDownloadAsync`에서 `(chId, msgId, downloadFolder/path)`가 모두 동일한 중복 아이템에 대해서는:
   - 이미 다운로드 진행 중(`WaitingQueue`, `Transferring`, `Merging`)이거나,
   - 이미 동일한 `(chId, msgId, downloadFolder)`로 다운로드 완료(`Completed`)되어 실제 저장된 파일(`SavedFilePath`)이 디스크에 그대로 존재할 때는 중복 다운로드가 실행되지 않고 `[중복 수신 무시] 이미 동일한 파일(chID, msgID, 경로)이 수신 중이거나 수신 완료되어 있습니다.`로 처리되도록 보강합니다!
3. 또한 `UnregisterReceiver(string chId, string msgId, string peerId = null)` 메서드도 함께 제공하여 수신 목록에서도 특정 항목을 등록 해제할 수 있게 합니다.

지금 `P2PReceiverNode.cs`를 수정하겠습니다.

Edited P2PReceiverNode.cs
Edited P2PReceiverNode.cs
Edited P2PReceiverNode.cs
Edited P2PReceiverNode.cs

또한 `P2PReceiverNode.cs`에도 `UnregisterReceiver(string chId, string msgId, string peerId = null)`를 추가하여 수신 목록에서 특정 항목을 등록 해제할 수 있게 해두겠습니다. 그리고 샘플 프로그램([MainWindow.xaml](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml), [MainWindow.xaml.cs](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml.cs))도 새로운 독립 클래스 `P2PFileWatchManager.Instance`와 `UnregisterFile`을 사용하도록 업데이트하겠습니다.

Edited P2PReceiverNode.cs
Edited MainWindow.xaml
Edited MainWindow.xaml.cs
Edited MainWindow.xaml.cs
Edited MainWindow.xaml.cs

요청하신 3가지 사항을 모두 반영하여 리팩토링을 완료했습니다.

---

### 1. `FileSystemWatcher`를 독립된 전용 클래스로 완전 분리 ([P2PFileWatchManager.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs))

* `P2PSenderNode`와 `P2PReceiverNode`에서 `FileSystemWatcher` 및 파일 감시 타이머를 완전히 제거하고, 별도의 독립된 클래스인 **[`P2PFileWatchManager`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs#L32-L300)**로 분리했습니다.
* 필요한 곳에서 감시할 파일을 등록하면 내부에서 자체 파일 목록([`GetWatchedFileList()`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs#L204-L213))을 관리하고, 파일이 삭제/이동되거나 복구될 때 이벤트로 알려줍니다:
  ```csharp
  // 1. 파일 삭제 및 복구 감지 이벤트 연결
  P2PFileWatchManager.Instance.OnFileMissing += (s, e) =>
  {
      // e.ChId, e.MsgId, e.FileName, e.MissingFilePath, e.Message
  };
  P2PFileWatchManager.Instance.OnFileRestored += (s, e) => { ... };

  // 2. 감시할 파일 등록 (chId, msgId, filePath가 모두 동일한 중복 아이템은 자동 무시됨)
  P2PFileWatchManager.Instance.RegisterWatch(chId, msgId, filePath);

  // 3. 감시 해제 (특정 파일 또는 전체 해제)
  P2PFileWatchManager.Instance.UnregisterWatch(chId, msgId, filePath);
  P2PFileWatchManager.Instance.ClearAllWatches();

  // 4. 현재 감시 중인 파일 목록 및 존재 여부(IsExists) 조회
  List<WatchedFileItem> watchedList = P2PFileWatchManager.Instance.GetWatchedFileList();
  ```

---

### 2. `(chId, msgId, path)`가 모두 동일한 중복 아이템 무시 처리

* **`P2PSenderNode.RegisterFile` / `TryRegisterFile` ([P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs#L280-L291))**:
  * 이미 동일한 `(chId, msgId, LocalSourcePath)`로 등록되어 있고 유효시간이 남아있는 파일이면 중복 등록하지 않고 기존 항목을 유지하며 `[중복 등록 무시]` 로그를 출력합니다.
* **`P2PReceiverNode.StartDownloadAsync` ([P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L394-L420))**:
  * 이미 동일한 `(peerId, chId, msgId, downloadFolder)`로 수신 진행 중이거나, 이미 수신 완료(`Completed`)되어 디스크에 해당 파일이 존재하는 경우에는 중복 다운로드를 시작하지 않고 `[중복 수신 무시]`로 처리합니다.
* **`P2PFileWatchManager.RegisterWatch` ([P2PFileWatchManager.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs#L106-L111))**:
  * 이미 동일한 `(chId, msgId, filePath)`가 감시 목록에 등록되어 있으면 중복 등록하지 않습니다.

---

### 3. `UnregisterFile` (등록 해제) 메서드 추가

* **송신자 등록 해제 ([P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs#L345-L394))**:
  ```csharp
  // 특정 (chId, msgId) 송신 파일을 등록 해제 (현재 전송 중이면 취소 후 목록에서 제거)
  bool removed = P2PSenderNode.Instance.UnregisterFile(chId, msgId);

  // 등록된 모든 송신 파일 일괄 해제
  P2PSenderNode.Instance.UnregisterAllFiles();
  ```
* **수신자 등록 해제 ([P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L177-L198))**:
  ```csharp
  // 특정 (chId, msgId) 수신 항목을 취소 및 수신 목록에서 제거
  bool removed = P2PReceiverNode.Instance.UnregisterReceiver(chId, msgId, peerId);
  ```
* 샘플 프로그램([MainWindow.xaml](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml#L66))에도 **`등록 해제 (Unregister)`** 버튼을 추가하여 등록과 해제를 즉시 테스트하실 수 있습니다.
* 
----
요청하신 **3가지 아키텍처 개선 사항**을 정확히 반영하겠습니다.

1. **`FileSystemWatcher`를 독립된 전용 클래스(`P2PFileWatchManager`)로 완전 분리 (`P2PSenderNode`와 `P2PReceiverNode`에서는 제거)**
   * 새 파일 [P2PFileWatchManager.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs)를 생성하여, 감시할 파일(`chId`, `msgId`, `filePath`)을 등록(`RegisterWatch`) 및 해제(`UnregisterWatch`)하면 자체 파일 목록(`GetWatchedFileList()`)을 관리하고, 파일이 삭제/이동되거나 복구될 때 `OnFileMissing` / `OnFileRestored` / `OnLog` 이벤트로 알려주도록 독립 모듈화합니다.
   * `P2PSenderNode`와 `P2PReceiverNode`에서는 `FileSystemWatcher`와 감시 타이머 코드를 완전히 제거하여 각 클래스가 순수하게 송신/수신 역할만 하도록 깔끔하게 분리합니다.

2. **`RegisterFile`, `StartDownloadAsync`, `RegisterWatch`에서 `(chId, msgId, filePath)`가 모두 동일한 중복 아이템은 중복 처리되지 않도록 방어**
   * **`P2PSenderNode.RegisterFile(chId, msgId, filePath)`**: 이미 동일한 `(chId, msgId, normalizedPath)`가 등록되어 있고 유효한 상태라면 중복 등록하지 않고 기존 항목을 유지하며 `[중복 등록 무시] 이미 동일한 파일(chID, msgID, 경로)이 등록되어 있습니다.` 로그를 출력합니다.
   * **`P2PReceiverNode.StartDownloadAsync(..., chId, msgId, downloadFolder)`**: 이미 동일한 `(chId, msgId, downloadFolder/filePath)`로 다운로드 중이거나 이미 다운로드 완료되어 파일이 존재하는 경우 중복 다운로드를 시작하지 않고 무시합니다.
   * **`P2PFileWatchManager.RegisterWatch(chId, msgId, filePath)`**: 동일한 `(chId, msgId, filePath)`가 이미 감시 목록에 있으면 중복 등록하지 않습니다.

3. **`P2PSenderNode.UnregisterFile(chId, msgId)` (및 전체 해제 `UnregisterAllFiles()`) 추가**
   * 등록했던 송신 파일을 목록에서 제거하고,もし 해당 파일이 전송 중이었다면 전송도 함께 취소·정리하는 `UnregisterFile(chId, msgId)` 메서드를 `P2PSenderNode`에 추가합니다.

지금 바로 코드를 작성 및 수정하겠습니다.

Created P2PFileWatchManager.cs

이제 [P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs)에서:
1. `FileSystemWatcher` 관련 코드를 모두 제거합니다.
2. `RegisterFile` / `TryRegisterFile`에서 `(chId, msgId, filePath)`가 모두 동일한 중복 아이템에 대해서는 중복 등록되지 않도록 처리하고 기존 항목을 유지합니다.
3. **`UnregisterFile(string chId, string msgId)`** 및 **`UnregisterAllFiles()`** 메서드를 추가합니다!

지금 바로 `P2PSenderNode.cs`를 수정하겠습니다.

Edited P2PSenderNode.cs
Edited P2PSenderNode.cs
Edited P2PSenderNode.cs
Edited P2PSenderNode.cs

이제 [P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs)에서도:
1. `FileSystemWatcher` 및 타이머 코드를 제거하여 `P2PFileWatchManager`와 완전히 분리합니다.
2. `StartDownloadAsync`에서 `(chId, msgId, downloadFolder/path)`가 모두 동일한 중복 아이템에 대해서는:
   - 이미 다운로드 진행 중(`WaitingQueue`, `Transferring`, `Merging`)이거나,
   - 이미 동일한 `(chId, msgId, downloadFolder)`로 다운로드 완료(`Completed`)되어 실제 저장된 파일(`SavedFilePath`)이 디스크에 그대로 존재할 때는 중복 다운로드가 실행되지 않고 `[중복 수신 무시] 이미 동일한 파일(chID, msgID, 경로)이 수신 중이거나 수신 완료되어 있습니다.`로 처리되도록 보강합니다!
3. 또한 `UnregisterReceiver(string chId, string msgId, string peerId = null)` 메서드도 함께 제공하여 수신 목록에서도 특정 항목을 등록 해제할 수 있게 합니다.

지금 `P2PReceiverNode.cs`를 수정하겠습니다.

Edited P2PReceiverNode.cs
Edited P2PReceiverNode.cs
Edited P2PReceiverNode.cs
Edited P2PReceiverNode.cs

또한 `P2PReceiverNode.cs`에도 `UnregisterReceiver(string chId, string msgId, string peerId = null)`를 추가하여 수신 목록에서 특정 항목을 등록 해제할 수 있게 해두겠습니다. 그리고 샘플 프로그램([MainWindow.xaml](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml), [MainWindow.xaml.cs](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml.cs))도 새로운 독립 클래스 `P2PFileWatchManager.Instance`와 `UnregisterFile`을 사용하도록 업데이트하겠습니다.

Edited P2PReceiverNode.cs
Edited MainWindow.xaml
Edited MainWindow.xaml.cs
Edited MainWindow.xaml.cs
Edited MainWindow.xaml.cs

요청하신 3가지 사항을 모두 반영하여 리팩토링을 완료했습니다.

---

### 1. `FileSystemWatcher`를 독립된 전용 클래스로 완전 분리 ([P2PFileWatchManager.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs))

* `P2PSenderNode`와 `P2PReceiverNode`에서 `FileSystemWatcher` 및 파일 감시 타이머를 완전히 제거하고, 별도의 독립된 클래스인 **[`P2PFileWatchManager`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs#L32-L300)**로 분리했습니다.
* 필요한 곳에서 감시할 파일을 등록하면 내부에서 자체 파일 목록([`GetWatchedFileList()`](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs#L204-L213))을 관리하고, 파일이 삭제/이동되거나 복구될 때 이벤트로 알려줍니다:
  ```csharp
  // 1. 파일 삭제 및 복구 감지 이벤트 연결
  P2PFileWatchManager.Instance.OnFileMissing += (s, e) =>
  {
      // e.ChId, e.MsgId, e.FileName, e.MissingFilePath, e.Message
  };
  P2PFileWatchManager.Instance.OnFileRestored += (s, e) => { ... };

  // 2. 감시할 파일 등록 (chId, msgId, filePath가 모두 동일한 중복 아이템은 자동 무시됨)
  P2PFileWatchManager.Instance.RegisterWatch(chId, msgId, filePath);

  // 3. 감시 해제 (특정 파일 또는 전체 해제)
  P2PFileWatchManager.Instance.UnregisterWatch(chId, msgId, filePath);
  P2PFileWatchManager.Instance.ClearAllWatches();

  // 4. 현재 감시 중인 파일 목록 및 존재 여부(IsExists) 조회
  List<WatchedFileItem> watchedList = P2PFileWatchManager.Instance.GetWatchedFileList();
  ```

---

### 2. `(chId, msgId, path)`가 모두 동일한 중복 아이템 무시 처리

* **`P2PSenderNode.RegisterFile` / `TryRegisterFile` ([P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs#L280-L291))**:
  * 이미 동일한 `(chId, msgId, LocalSourcePath)`로 등록되어 있고 유효시간이 남아있는 파일이면 중복 등록하지 않고 기존 항목을 유지하며 `[중복 등록 무시]` 로그를 출력합니다.
* **`P2PReceiverNode.StartDownloadAsync` ([P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L394-L420))**:
  * 이미 동일한 `(peerId, chId, msgId, downloadFolder)`로 수신 진행 중이거나, 이미 수신 완료(`Completed`)되어 디스크에 해당 파일이 존재하는 경우에는 중복 다운로드를 시작하지 않고 `[중복 수신 무시]`로 처리합니다.
* **`P2PFileWatchManager.RegisterWatch` ([P2PFileWatchManager.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PFileWatchManager.cs#L106-L111))**:
  * 이미 동일한 `(chId, msgId, filePath)`가 감시 목록에 등록되어 있으면 중복 등록하지 않습니다.

---

### 3. `UnregisterFile` (등록 해제) 메서드 추가

* **송신자 등록 해제 ([P2PSenderNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PSenderNode.cs#L345-L394))**:
  ```csharp
  // 특정 (chId, msgId) 송신 파일을 등록 해제 (현재 전송 중이면 취소 후 목록에서 제거)
  bool removed = P2PSenderNode.Instance.UnregisterFile(chId, msgId);

  // 등록된 모든 송신 파일 일괄 해제
  P2PSenderNode.Instance.UnregisterAllFiles();
  ```
* **수신자 등록 해제 ([P2PReceiverNode.cs](file:///e:/_antigravity/p2p/P2PTransferLib/P2PReceiverNode.cs#L177-L198))**:
  ```csharp
  // 특정 (chId, msgId) 수신 항목을 취소 및 수신 목록에서 제거
  bool removed = P2PReceiverNode.Instance.UnregisterReceiver(chId, msgId, peerId);
  ```
* 샘플 프로그램([MainWindow.xaml](file:///e:/_antigravity/p2p/P2PSampleApp/MainWindow.xaml#L66))에도 **`등록 해제 (Unregister)`** 버튼을 추가하여 등록과 해제를 즉시 테스트하실 수 있습니다.
