using System;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace P2PTransferLib
{
    /// <summary>
    /// 파일 조각(Chunk)의 간단한 암호화(XOR 대칭 난독화) 및 무결성 체크(SHA-256 해시 검증) 유틸리티
    /// </summary>
    public static class ChunkSecurityHelper
    {
        private static readonly byte[] SecretKey = Encoding.UTF8.GetBytes("P2P_SIMPLE_CRYPTO_KEY_2026!@#");

        /// <summary>
        /// 원본 바이트 배열에 대해 SHA-256 무결성 해시(32바이트)를 계산합니다.
        /// </summary>
        public static byte[] ComputeSha256(byte[] data, int offset, int count)
        {
            using (var sha = SHA256.Create())
            {
                return sha.ComputeHash(data, offset, count);
            }
        }

        /// <summary>
        /// 파일 조각 데이터를 간단하게 암호화/복호화(XOR + 청크 인덱스 솔트) 처리합니다.
        /// </summary>
        public static byte[] TransformChunk(byte[] input, int count, int chunkIndex)
        {
            byte[] output = new byte[count];
            int keyLen = SecretKey.Length;
            byte indexSalt = (byte)((chunkIndex * 31 + 17) & 0xFF);

            for (int i = 0; i < count; i++)
            {
                output[i] = (byte)(input[i] ^ SecretKey[i % keyLen] ^ indexSalt ^ (byte)(i & 0xFF));
            }
            return output;
        }

        /// <summary>
        /// 두 해시 바이트 배열이 동일한지 검증합니다.
        /// </summary>
        public static bool VerifyHash(byte[] hash1, byte[] hash2)
        {
            if (hash1 == null || hash2 == null || hash1.Length != hash2.Length)
                return false;

            for (int i = 0; i < hash1.Length; i++)
            {
                if (hash1[i] != hash2[i])
                    return false;
            }
            return true;
        }
    }

    /// <summary>
    /// P2P 바이너리 프레임 구조체
    /// [고정 헤더 45 Bytes]
    /// - [0..3]   (4B) : StartCode (Magic Bytes: 0x50, 0x32, 0x50, 0x21 = "P2P!", 프레임 시작점 식별 및 스트림 오염 방지)
    /// - [4]      (1B) : P2PCommand (제어명령인지 데이터 패킷인지 구분: RequestChunk, DataChunk, Pause, Resume, Cancel, WaitQueue 등)
    /// - [5..8]   (4B) : ChunkIndex (패킷/조각 번호. 패킷 크기가 64KB로 일정하므로 이 번호만으로 양측이 진행 상태를 정확히 산출)
    /// - [9..12]  (4B) : PayloadLength (뒤따르는 바이트 길이)
    /// - [13..44] (32B): IntegrityHash (DataChunk일 때 원본 조각의 SHA-256 해시)
    /// [가변 페이로드 N Bytes]
    /// - DataChunk일 때: 암호화된 파일 조각 바이트 (기본 64KB)
    /// - 제어 명령일 때: UTF-8 텍스트 또는 JSON 메타데이터
    /// </summary>
    public class P2PFrame
    {
        public P2PCommand Command { get; set; }
        public int ChunkIndex { get; set; }
        public byte[] Hash { get; set; } // 32 bytes
        public byte[] Payload { get; set; }

        public string GetPayloadText()
        {
            if (Payload == null || Payload.Length == 0) return string.Empty;
            return Encoding.UTF8.GetString(Payload);
        }
    }

    /// <summary>
    /// P2P 프로토콜 프레임 입출력 도우미 (스타트 코드 검증 포함)
    /// </summary>
    public static class FrameStreamHelper
    {
        /// <summary>
        /// 프레임 시작을 알리는 4바이트 스타트 코드 ("P2P!" = 0x21503250 Little-Endian)
        /// </summary>
        public const int StartCodeMagic = 0x21503250;

        /// <summary>
        /// 고정 헤더 크기: 4B(StartCode) + 1B(Command) + 4B(ChunkIndex) + 4B(PayloadLen) + 32B(SHA256) = 45 Bytes
        /// </summary>
        public const int HeaderSize = 45;

        public static string SerializeJson<T>(T obj)
        {
            if (obj == null) return string.Empty;
            var serializer = new DataContractJsonSerializer(typeof(T));
            using (var ms = new MemoryStream())
            {
                serializer.WriteObject(ms, obj);
                return Encoding.UTF8.GetString(ms.ToArray());
            }
        }

        public static T DeserializeJson<T>(string json)
        {
            if (string.IsNullOrWhiteSpace(json)) return default(T);
            var serializer = new DataContractJsonSerializer(typeof(T));
            byte[] bytes = Encoding.UTF8.GetBytes(json);
            using (var ms = new MemoryStream(bytes))
            {
                return (T)serializer.ReadObject(ms);
            }
        }

        /// <summary>
        /// 스타트 코드(StartCode)가 포함된 제어 명령 또는 데이터 패킷 프레임을 스트림에 씁니다.
        /// </summary>
        public static async Task WriteFrameAsync(Stream stream, P2PCommand cmd, int chunkIndex, byte[] hash32, byte[] payload, CancellationToken ct)
        {
            byte[] header = new byte[HeaderSize];

            // [0..3] StartCode (4 Bytes)
            Buffer.BlockCopy(BitConverter.GetBytes(StartCodeMagic), 0, header, 0, 4);

            // [4] Command (1 Byte)
            header[4] = (byte)cmd;

            // [5..8] ChunkIndex (4 Bytes)
            int payloadLen = payload != null ? payload.Length : 0;
            Buffer.BlockCopy(BitConverter.GetBytes(chunkIndex), 0, header, 5, 4);

            // [9..12] PayloadLength (4 Bytes)
            Buffer.BlockCopy(BitConverter.GetBytes(payloadLen), 0, header, 9, 4);

            // [13..44] SHA-256 Hash (32 Bytes)
            if (hash32 != null && hash32.Length == 32)
            {
                Buffer.BlockCopy(hash32, 0, header, 13, 32);
            }

            await stream.WriteAsync(header, 0, HeaderSize, ct).ConfigureAwait(false);
            if (payloadLen > 0)
            {
                await stream.WriteAsync(payload, 0, payloadLen, ct).ConfigureAwait(false);
            }
            await stream.FlushAsync(ct).ConfigureAwait(false);
        }

        /// <summary>
        /// 텍스트 페이로드를 가진 제어 명령 프레임을 전송합니다.
        /// </summary>
        public static Task WriteControlFrameAsync(Stream stream, P2PCommand cmd, int chunkIndex, string textPayload, CancellationToken ct)
        {
            byte[] payload = string.IsNullOrEmpty(textPayload) ? null : Encoding.UTF8.GetBytes(textPayload);
            return WriteFrameAsync(stream, cmd, chunkIndex, null, payload, ct);
        }

        /// <summary>
        /// 스트림에서 P2PFrame(헤더 45바이트 + 페이로드)을 읽어오며 스타트 코드(StartCodeMagic)를 검증합니다.
        /// </summary>
        public static async Task<P2PFrame> ReadFrameAsync(Stream stream, CancellationToken ct)
        {
            byte[] header = await ReadExactAsync(stream, HeaderSize, ct).ConfigureAwait(false);
            if (header == null) return null;

            // 1. 스타트 코드("P2P!") 검증
            int startCode = BitConverter.ToInt32(header, 0);
            if (startCode != StartCodeMagic)
            {
                throw new InvalidDataException($"잘못된 패킷 스타트 코드(0x{startCode:X8})가 감지되었습니다. 스트림 동기화 오류!");
            }

            // 2. 명령/패킷 구분자, 조각 번호, 페이로드 길이 추출
            P2PCommand cmd = (P2PCommand)header[4];
            int chunkIndex = BitConverter.ToInt32(header, 5);
            int payloadLen = BitConverter.ToInt32(header, 9);

            if (payloadLen < 0 || payloadLen > 16 * 1024 * 1024)
            {
                throw new InvalidDataException($"비정상적인 패킷 페이로드 길이({payloadLen} Bytes)가 감지되었습니다.");
            }

            // 3. SHA-256 해시 추출
            byte[] hash = new byte[32];
            Buffer.BlockCopy(header, 13, hash, 0, 32);

            // 4. 페이로드 읽기
            byte[] payload = null;
            if (payloadLen > 0)
            {
                payload = await ReadExactAsync(stream, payloadLen, ct).ConfigureAwait(false);
                if (payload == null) return null;
            }

            return new P2PFrame
            {
                Command = cmd,
                ChunkIndex = chunkIndex,
                Hash = hash,
                Payload = payload
            };
        }

        /// <summary>
        /// 지정된 바이트 수만큼 정확히 스트림에서 읽어옵니다.
        /// </summary>
        public static async Task<byte[]> ReadExactAsync(Stream stream, int count, CancellationToken ct)
        {
            byte[] buffer = new byte[count];
            int offset = 0;
            while (offset < count)
            {
                int read = await stream.ReadAsync(buffer, offset, count - offset, ct).ConfigureAwait(false);
                if (read == 0)
                {
                    return null;
                }
                offset += read;
            }
            return buffer;
        }
    }
}
