using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace P2PTransferLib
{
    public static class P2PSelfTestRunner
    {
        public static async Task<string> RunEndToEndVerificationAsync()
        {
            string tempRoot = Path.Combine(Path.GetTempPath(), "P2PVerify_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(tempRoot);

            int testP2PPort = 19105;

            var sender = new P2PSenderNode
            {
                ChunkSize = 64 * 1024,
                MaxConcurrentReceivers = 3,
                SpeedLimitKbps = 0
            };

            var receiver = new P2PReceiverNode();

            try
            {
                sender.StartServer(testP2PPort, "127.0.0.1");

                string srcFile1 = Path.Combine(tempRoot, "FileA.bin");
                string srcFile2 = Path.Combine(tempRoot, "FileB.bin");
                byte[] data1 = new byte[200 * 1024];
                byte[] data2 = new byte[150 * 1024];
                new Random(1).NextBytes(data1);
                new Random(2).NextBytes(data2);
                File.WriteAllBytes(srcFile1, data1);
                File.WriteAllBytes(srcFile2, data2);

                sender.RegisterFile("CH_01", "MSG_001", srcFile1);
                sender.RegisterFile("CH_01", "MSG_002", srcFile2);

                string dlDir = Path.Combine(tempRoot, "Downloads");

                // 5개 요청을 동시에 시작 (3개는 즉시 전송, 2개는 대기 후 전송)
                var t1 = receiver.StartDownloadAsync("127.0.0.1", testP2PPort, "EMP_101", "김철수", "CH_01", "MSG_001", dlDir);
                var t2 = receiver.StartDownloadAsync("127.0.0.1", testP2PPort, "EMP_102", "이영희", "CH_01", "MSG_001", dlDir);
                var t3 = receiver.StartDownloadAsync("127.0.0.1", testP2PPort, "EMP_103", "박민수", "CH_01", "MSG_002", dlDir);
                var t4 = receiver.StartDownloadAsync("127.0.0.1", testP2PPort, "EMP_104", "최지훈", "CH_01", "MSG_002", dlDir);
                var t5 = receiver.StartDownloadAsync("127.0.0.1", testP2PPort, "EMP_105", "정수진", "CH_01", "MSG_001", dlDir);

                await Task.WhenAll(t1, t2, t3, t4, t5);

                return "OK";
            }
            finally
            {
                receiver.Dispose();
                sender.Dispose();
                try { Directory.Delete(tempRoot, true); } catch { }
            }
        }
    }
}
