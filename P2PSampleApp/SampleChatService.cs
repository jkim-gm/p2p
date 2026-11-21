using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using P2PTransferLib;

namespace P2PSampleApp
{
    /// <summary>
    /// 샘플 프로그램(WPF) 내부에서 동작하는 가벼운 채팅 메시지 구조체
    /// - "CHAT" : 일반 대화
    /// - "FILE_NOTICE" : 최초 파일 전송이 있다고 알림 (파일명/크기 목록/유효시간)
    /// - "PRE_CANCEL" : 수신자가 수신 시작(P2P 연결 수립)을 누르기 전에 수신취소하거나 송신자가 송신취소할 때 전달
    /// (일단 수신 시작을 눌러 P2P 연결이 수립된 이후에는 P2P 포트의 헤더 제어 명령으로 취소/일시정지/재개가 직접 처리됨)
    /// </summary>
    [Serializable]
    public class SampleChatPacket
    {
        public string Type { get; set; } // "HELLO", "CHAT", "FILE_NOTICE", "PRE_CANCEL"
        public string SenderId { get; set; }
        public string SenderName { get; set; }
        public string TargetPeerId { get; set; } // null/empty = 전체 브로드캐스트
        public string TransferId { get; set; }
        public string Text { get; set; }
        public TransferOffer FileNotice { get; set; }
        public long TimestampTicks { get; set; }
    }

    public class SampleChatEventArgs : EventArgs
    {
        public string Type { get; set; }
        public string SenderId { get; set; }
        public string SenderName { get; set; }
        public string TargetPeerId { get; set; }
        public string TransferId { get; set; }
        public string Message { get; set; }
        public DateTime Timestamp { get; set; }
        public TransferOffer FileNotice { get; set; }
    }

    internal static class SampleChatFraming
    {
        public static string ToJson<T>(T obj)
        {
            if (obj == null) return string.Empty;
            var ser = new DataContractJsonSerializer(typeof(T));
            using (var ms = new MemoryStream())
            {
                ser.WriteObject(ms, obj);
                return Encoding.UTF8.GetString(ms.ToArray());
            }
        }

        public static T FromJson<T>(string json)
        {
            if (string.IsNullOrWhiteSpace(json)) return default(T);
            var ser = new DataContractJsonSerializer(typeof(T));
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            using (var ms = new MemoryStream(bytes))
            {
                return (T)ser.ReadObject(ms);
            }
        }

        public static async Task WriteFrameAsync(NetworkStream stream, string json, CancellationToken ct)
        {
            byte[] data = Encoding.UTF8.GetBytes(json);
            byte[] len = BitConverter.GetBytes(data.Length);
            await stream.WriteAsync(len, 0, 4, ct).ConfigureAwait(false);
            if (data.Length > 0)
            {
                await stream.WriteAsync(data, 0, data.Length, ct).ConfigureAwait(false);
            }
            await stream.FlushAsync(ct).ConfigureAwait(false);
        }

        public static async Task<string> ReadFrameAsync(NetworkStream stream, CancellationToken ct)
        {
            byte[] lenBuf = await ReadExactAsync(stream, 4, ct).ConfigureAwait(false);
            if (lenBuf == null) return null;
            int len = BitConverter.ToInt32(lenBuf, 0);
            if (len <= 0 || len > 10 * 1024 * 1024) return null;
            byte[] data = await ReadExactAsync(stream, len, ct).ConfigureAwait(false);
            if (data == null) return null;
            return Encoding.UTF8.GetString(data);
        }

        private static async Task<byte[]> ReadExactAsync(Stream stream, int count, CancellationToken ct)
        {
            byte[] buf = new byte[count];
            int offset = 0;
            while (offset < count)
            {
                int r = await stream.ReadAsync(buf, offset, count - offset, ct).ConfigureAwait(false);
                if (r == 0) return null;
                offset += r;
            }
            return buf;
        }
    }

    public class ConnectedPeerInfo : EventArgs
    {
        public string PeerId { get; set; }
        public string PeerName { get; set; }
        public bool IsConnected { get; set; }
    }

    /// <summary>
    /// 샘플 프로그램의 송신자(A) 채팅 서버
    /// (일반 텍스트 채팅, 최초 파일명/크기 알림, P2P 연결 수립 전 취소 알림을 수행)
    /// </summary>
    public class SampleChatServer : IDisposable
    {
        public string ServerId { get; } = "SENDER_A";
        public string ServerName { get; set; } = "A (송신서버)";
        public int Port { get; private set; }
        public bool IsRunning { get; private set; }

        private TcpListener _listener;
        private CancellationTokenSource _cts;
        private readonly ConcurrentDictionary<string, ClientConn> _clients = new ConcurrentDictionary<string, ClientConn>();

        public event EventHandler<SampleChatEventArgs> MessageReceived;
        public event EventHandler<ConnectedPeerInfo> PeerConnectionChanged;

        private class ClientConn
        {
            public string PeerId { get; set; }
            public string PeerName { get; set; }
            public TcpClient Client { get; set; }
            public NetworkStream Stream { get; set; }
            public SemaphoreSlim Lock { get; } = new SemaphoreSlim(1, 1);
        }

        public List<ConnectedPeerInfo> GetConnectedPeers()
        {
            return _clients.Values.Select(c => new ConnectedPeerInfo
            {
                PeerId = c.PeerId,
                PeerName = c.PeerName,
                IsConnected = true
            }).ToList();
        }

        public void Start(int port, string bindIp = "127.0.0.1")
        {
            if (IsRunning) return;
            Port = port;
            _cts = new CancellationTokenSource();

            IPAddress ip = IPAddress.Any;
            if (!string.IsNullOrWhiteSpace(bindIp) &&
                (bindIp.Trim() == "127.0.0.1" || bindIp.Trim().Equals("localhost", StringComparison.OrdinalIgnoreCase)))
            {
                ip = IPAddress.Loopback;
            }

            _listener = new TcpListener(ip, port);
            _listener.Start();
            IsRunning = true;

            Task.Run(() => AcceptLoopAsync(_cts.Token));
        }

        public void Stop()
        {
            IsRunning = false;
            try { _cts?.Cancel(); } catch { }
            try { _listener?.Stop(); } catch { }

            foreach (var kv in _clients)
            {
                try { kv.Value.Client?.Close(); } catch { }
            }
            _clients.Clear();
        }

        private async Task AcceptLoopAsync(CancellationToken ct)
        {
            while (!ct.IsCancellationRequested && IsRunning)
            {
                try
                {
                    var client = await _listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    _ = Task.Run(() => HandleClientAsync(client, ct));
                }
                catch
                {
                    if (ct.IsCancellationRequested || !IsRunning) break;
                }
            }
        }

        private async Task HandleClientAsync(TcpClient tcp, CancellationToken ct)
        {
            string peerId = null;
            string peerName = "Unknown";

            try
            {
                var stream = tcp.GetStream();
                while (!ct.IsCancellationRequested && tcp.Connected)
                {
                    string json = await SampleChatFraming.ReadFrameAsync(stream, ct).ConfigureAwait(false);
                    if (json == null) break;

                    var pkt = SampleChatFraming.FromJson<SampleChatPacket>(json);
                    if (pkt == null) continue;

                    if (pkt.Type == "HELLO")
                    {
                        peerId = pkt.SenderId;
                        peerName = pkt.SenderName;
                        _clients[peerId] = new ClientConn
                        {
                            PeerId = peerId,
                            PeerName = peerName,
                            Client = tcp,
                            Stream = stream
                        };
                        PeerConnectionChanged?.Invoke(this, new ConnectedPeerInfo { PeerId = peerId, PeerName = peerName, IsConnected = true });
                        RaiseMessage("CHAT", "SYSTEM", "시스템", $"{peerName} 님이 채팅에 연결되었습니다.", null, null);
                    }
                    else if (pkt.Type == "CHAT")
                    {
                        RaiseMessage("CHAT", pkt.SenderId, pkt.SenderName, pkt.Text, null, null);
                        await BroadcastRawAsync(json).ConfigureAwait(false);
                    }
                    else if (pkt.Type == "PRE_CANCEL")
                    {
                        // 수신자가 수신 시작(P2P 연결) 전에 취소한 경우
                        RaiseMessage("PRE_CANCEL", pkt.SenderId, pkt.SenderName, pkt.Text, null, pkt.TransferId);
                    }
                }
            }
            catch { }
            finally
            {
                if (peerId != null && _clients.TryRemove(peerId, out _))
                {
                    PeerConnectionChanged?.Invoke(this, new ConnectedPeerInfo { PeerId = peerId, PeerName = peerName, IsConnected = false });
                    RaiseMessage("CHAT", "SYSTEM", "시스템", $"{peerName} 님의 채팅 연결이 종료되었습니다.", null, null);
                }
                try { tcp.Close(); } catch { }
            }
        }

        public async Task BroadcastChatTextAsync(string text)
        {
            var pkt = new SampleChatPacket
            {
                Type = "CHAT",
                SenderId = ServerId,
                SenderName = ServerName,
                Text = text,
                TimestampTicks = DateTime.Now.Ticks
            };
            RaiseMessage("CHAT", ServerId, ServerName, text, null, null);
            await BroadcastRawAsync(SampleChatFraming.ToJson(pkt)).ConfigureAwait(false);
        }

        /// <summary>
        /// 채팅을 통해 최초 "보낼 파일이 있다(파일명/크기 목록)"는 알림만 수신자들에게 보냅니다.
        /// </summary>
        public async Task BroadcastFileNoticeAsync(TransferOffer offer, string customMessage = null)
        {
            string fileSummary = string.Join(", ", offer.Files.Select(f => $"{f.FileName} ({P2PSenderNode.FormatBytes(f.FileSize)})"));
            string msg = string.IsNullOrWhiteSpace(customMessage)
                ? $"[파일 전송 알림] {offer.Files.Count}개 파일: {fileSummary} (유효시간 30분, 패킷크기:64KB)"
                : $"{customMessage} [파일: {fileSummary}]";

            var wireOffer = new TransferOffer
            {
                TransferId = offer.TransferId,
                SenderName = offer.SenderName,
                P2PPort = offer.P2PPort,
                ChunkSize = offer.ChunkSize,
                CreatedUtcTicks = offer.CreatedUtcTicks,
                ExpireUtcTicks = offer.ExpireUtcTicks,
                Files = offer.Files.Select(f => new FileItemMeta
                {
                    FileId = f.FileId,
                    FileName = f.FileName,
                    FileSize = f.FileSize,
                    ChunkSize = f.ChunkSize,
                    TotalChunks = f.TotalChunks,
                    LocalSourcePath = null
                }).ToList()
            };

            var pkt = new SampleChatPacket
            {
                Type = "FILE_NOTICE",
                SenderId = ServerId,
                SenderName = ServerName,
                TransferId = offer.TransferId,
                Text = msg,
                FileNotice = wireOffer,
                TimestampTicks = DateTime.Now.Ticks
            };

            RaiseMessage("FILE_NOTICE", ServerId, ServerName, msg, null, offer.TransferId);
            await BroadcastRawAsync(SampleChatFraming.ToJson(pkt)).ConfigureAwait(false);
        }

        /// <summary>
        /// 송신자(A)가 P2P 연결이 수립되기 전인 수신자(또는 전체 수신자)에게 채팅으로 송신 취소를 알립니다.
        /// </summary>
        public async Task SendPreCancelNoticeAsync(string transferId, string targetPeerId = null)
        {
            string msg = string.IsNullOrEmpty(targetPeerId)
                ? "[송신 취소 알림] 송신자가 파일 전송을 취소했습니다."
                : $"[송신 취소 알림] 송신자가 {targetPeerId}에 대한 파일 전송을 취소했습니다.";

            var pkt = new SampleChatPacket
            {
                Type = "PRE_CANCEL",
                SenderId = ServerId,
                SenderName = ServerName,
                TargetPeerId = targetPeerId,
                TransferId = transferId,
                Text = msg,
                TimestampTicks = DateTime.Now.Ticks
            };

            RaiseMessage("CHAT", ServerId, ServerName, msg, null, transferId);
            string json = SampleChatFraming.ToJson(pkt);

            if (string.IsNullOrEmpty(targetPeerId))
            {
                await BroadcastRawAsync(json).ConfigureAwait(false);
            }
            else if (_clients.TryGetValue(targetPeerId, out var c))
            {
                await SendRawToClientAsync(c, json).ConfigureAwait(false);
            }
        }

        private async Task BroadcastRawAsync(string json)
        {
            foreach (var c in _clients.Values)
            {
                await SendRawToClientAsync(c, json).ConfigureAwait(false);
            }
        }

        private async Task SendRawToClientAsync(ClientConn c, string json)
        {
            await c.Lock.WaitAsync().ConfigureAwait(false);
            try
            {
                await SampleChatFraming.WriteFrameAsync(c.Stream, json, _cts?.Token ?? CancellationToken.None).ConfigureAwait(false);
            }
            catch { }
            finally
            {
                c.Lock.Release();
            }
        }

        private void RaiseMessage(string type, string id, string name, string msg, TransferOffer notice, string transferId)
        {
            MessageReceived?.Invoke(this, new SampleChatEventArgs
            {
                Type = type,
                SenderId = id,
                SenderName = name,
                TransferId = transferId,
                Message = msg,
                Timestamp = DateTime.Now,
                FileNotice = notice
            });
        }

        public void Dispose()
        {
            Stop();
        }
    }

    /// <summary>
    /// 샘플 프로그램의 수신자(B, C, D, E, F...) 채팅 클라이언트
    /// </summary>
    public class SampleChatClient : IDisposable
    {
        public string PeerId { get; }
        public string PeerName { get; }
        public string ServerIp { get; private set; }
        public int ServerPort { get; private set; }
        public bool IsConnected => _client != null && _client.Connected;

        private TcpClient _client;
        private NetworkStream _stream;
        private CancellationTokenSource _cts;
        private readonly SemaphoreSlim _writeLock = new SemaphoreSlim(1, 1);

        public event EventHandler<SampleChatEventArgs> MessageReceived;
        public event EventHandler<bool> ConnectionStateChanged;

        public SampleChatClient(string peerId, string peerName)
        {
            PeerId = peerId;
            PeerName = peerName;
        }

        public async Task ConnectAsync(string serverIp, int serverPort)
        {
            Disconnect();
            ServerIp = serverIp;
            ServerPort = serverPort;
            _cts = new CancellationTokenSource();

            _client = new TcpClient();
            await _client.ConnectAsync(serverIp, serverPort).ConfigureAwait(false);
            _stream = _client.GetStream();

            ConnectionStateChanged?.Invoke(this, true);

            var hello = new SampleChatPacket
            {
                Type = "HELLO",
                SenderId = PeerId,
                SenderName = PeerName,
                TimestampTicks = DateTime.Now.Ticks
            };
            await SendPacketAsync(hello).ConfigureAwait(false);

            _ = Task.Run(() => ReceiveLoopAsync(_cts.Token));
        }

        public void Disconnect()
        {
            try { _cts?.Cancel(); } catch { }
            try { _stream?.Close(); } catch { }
            try { _client?.Close(); } catch { }
            _stream = null;
            _client = null;
        }

        public async Task SendChatTextAsync(string text)
        {
            if (!IsConnected) throw new InvalidOperationException("채팅 서버에 연결되어 있지 않습니다.");
            var pkt = new SampleChatPacket
            {
                Type = "CHAT",
                SenderId = PeerId,
                SenderName = PeerName,
                Text = text,
                TimestampTicks = DateTime.Now.Ticks
            };
            await SendPacketAsync(pkt).ConfigureAwait(false);
        }

        /// <summary>
        /// 수신자가 아직 수신 시작(P2P 연결)을 누르기 전에 수신 취소(거절)를 하는 경우 채팅으로 송신자에게 알립니다.
        /// </summary>
        public async Task SendPreCancelNoticeAsync(string transferId)
        {
            if (!IsConnected) return;
            var pkt = new SampleChatPacket
            {
                Type = "PRE_CANCEL",
                SenderId = PeerId,
                SenderName = PeerName,
                TransferId = transferId,
                Text = $"[수신 취소] {PeerName} 님이 수신 시작 전에 파일 전송을 취소했습니다.",
                TimestampTicks = DateTime.Now.Ticks
            };
            await SendPacketAsync(pkt).ConfigureAwait(false);
        }

        private async Task SendPacketAsync(SampleChatPacket pkt)
        {
            if (_stream == null) return;
            string json = SampleChatFraming.ToJson(pkt);
            await _writeLock.WaitAsync().ConfigureAwait(false);
            try
            {
                await SampleChatFraming.WriteFrameAsync(_stream, json, _cts?.Token ?? CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                _writeLock.Release();
            }
        }

        private async Task ReceiveLoopAsync(CancellationToken ct)
        {
            try
            {
                while (!ct.IsCancellationRequested && _client != null && _client.Connected)
                {
                    string json = await SampleChatFraming.ReadFrameAsync(_stream, ct).ConfigureAwait(false);
                    if (json == null)
                    {
                        ConnectionStateChanged?.Invoke(this, false);
                        break;
                    }

                    var pkt = SampleChatFraming.FromJson<SampleChatPacket>(json);
                    if (pkt == null) continue;

                    if (pkt.Type == "CHAT")
                    {
                        MessageReceived?.Invoke(this, new SampleChatEventArgs
                        {
                            Type = "CHAT",
                            SenderId = pkt.SenderId,
                            SenderName = pkt.SenderName,
                            Message = pkt.Text,
                            Timestamp = DateTime.Now
                        });
                    }
                    else if (pkt.Type == "FILE_NOTICE" && pkt.FileNotice != null)
                    {
                        pkt.FileNotice.SenderIp = ServerIp;
                        MessageReceived?.Invoke(this, new SampleChatEventArgs
                        {
                            Type = "FILE_NOTICE",
                            SenderId = pkt.SenderId,
                            SenderName = pkt.SenderName,
                            TransferId = pkt.TransferId,
                            Message = pkt.Text,
                            Timestamp = DateTime.Now,
                            FileNotice = pkt.FileNotice
                        });
                    }
                    else if (pkt.Type == "PRE_CANCEL")
                    {
                        if (string.IsNullOrEmpty(pkt.TargetPeerId) || pkt.TargetPeerId == PeerId)
                        {
                            MessageReceived?.Invoke(this, new SampleChatEventArgs
                            {
                                Type = "PRE_CANCEL",
                                SenderId = pkt.SenderId,
                                SenderName = pkt.SenderName,
                                TargetPeerId = pkt.TargetPeerId,
                                TransferId = pkt.TransferId,
                                Message = pkt.Text,
                                Timestamp = DateTime.Now
                            });
                        }
                    }
                }
            }
            catch
            {
                if (!ct.IsCancellationRequested)
                {
                    ConnectionStateChanged?.Invoke(this, false);
                }
            }
        }

        public void Dispose()
        {
            Disconnect();
        }
    }
}
