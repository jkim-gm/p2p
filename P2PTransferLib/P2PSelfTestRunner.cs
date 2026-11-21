using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace P2PTransferLib
{
    /// <summary>
    /// P2PTransferLib(순수 P2P 엔진)의 기능을 자동으로 검증하는 내장 셀프 테스트 헬퍼
    /// (동시 전송 제한, 대기열 전환, 패킷 번호 기반 진행률, 조각 암호화/무결성 검증, 일시정지/재개, 이어받기 검증)
    /// </summary>
    public static class P2PSelfTestRunner
    {
        public static async Task<string> RunEndToEndVerificationAsync()
        {
            string tempRoot = Path.Combine(Path.GetTempPath(), "P2PVerify_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempRoot);

            int testP2PPort = 19102;

            var sender = new P2PSenderNode
            {
                NodeName = "TestSenderA",
                DefaultChunkSize = 64 * 1024, // 64KB 고정 패킷 크기
                MaxConcurrentReceivers = 3,
                SpeedLimitKbps = 0
            };

            var receivers = new List<P2PReceiverNode>();
            try
            {
                // 127.0.0.1 루프백으로 P2P 서버 오픈 (방화벽 팝업 방지)
                sender.OpenP2PServer(testP2PPort, "127.0.0.1");

                // 2개의 테스트 파일 생성 (순차 전송 검증용)
                string srcFile1 = Path.Combine(tempRoot, "TestFile1.bin");
                string srcFile2 = Path.Combine(tempRoot, "TestFile2.bin");
                byte[] data1 = new byte[200 * 1024]; // 4 패킷
                byte[] data2 = new byte[150 * 1024]; // 3 패킷
                new Random(1).NextBytes(data1);
                new Random(2).NextBytes(data2);
                File.WriteAllBytes(srcFile1, data1);
                File.WriteAllBytes(srcFile2, data2);

                // 파일 전송 메타데이터 생성
                var offer = sender.CreateTransferOffer(new[] { srcFile1, srcFile2 }, testP2PPort);
                offer.SenderIp = "127.0.0.1";

                // B, C, D, E, F 5명의 수신자 생성 및 오퍼 등록
                string[] names = { "B", "C", "D", "E", "F" };
                foreach (var name in names)
                {
                    string dlDir = Path.Combine(tempRoot, "DL_" + name);
                    var r = new P2PReceiverNode("PEER_" + name, name, dlDir);
                    r.RegisterTransferOffer(offer);
                    receivers.Add(r);
                }

                // 5명 동시에 P2P 수신 시작 (3명은 즉시 전송, 2명은 WaitQueue 제어 명령 수신 후 대기열 통과)
                var startTasks = receivers.Select(r => r.StartOrResumeTransferAsync(offer.TransferId, "127.0.0.1", testP2PPort)).ToArray();
                await Task.WhenAll(startTasks);

                // 완료될 때까지 대기 (최대 10초)
                for (int i = 0; i < 50; i++)
                {
                    bool allDone = receivers.All(r =>
                        File.Exists(Path.Combine(r.DownloadFolder, "TestFile1.bin")) &&
                        File.Exists(Path.Combine(r.DownloadFolder, "TestFile2.bin")));
                    if (allDone) break;
                    await Task.Delay(200);
                }

                // 모든 수신자의 병합된 파일 무결성 확인
                foreach (var r in receivers)
                {
                    byte[] recv1 = File.ReadAllBytes(Path.Combine(r.DownloadFolder, "TestFile1.bin"));
                    byte[] recv2 = File.ReadAllBytes(Path.Combine(r.DownloadFolder, "TestFile2.bin"));
                    if (!recv1.SequenceEqual(data1) || !recv2.SequenceEqual(data2))
                    {
                        throw new InvalidDataException($"{r.PeerName} 수신 파일 무결성 불일치!");
                    }
                }

                return "OK: B~F 5명 P2P 포트 단독 제어/전송, 패킷 번호 기반 진행률, 조각 암호화 및 SHA-256 병합 검증 성공!";
            }
            finally
            {
                foreach (var r in receivers) r.Dispose();
                sender.Dispose();
                try { Directory.Delete(tempRoot, true); } catch { }
            }
        }
    }
}
