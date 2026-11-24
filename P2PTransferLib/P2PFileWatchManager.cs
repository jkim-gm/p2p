using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace P2PTransferLib
{
    /// <summary>
    /// 감시 대상 개별 파일 항목
    /// </summary>
    public class WatchedFileItem
    {
        public string WatchKey { get; set; }
        public string ChId { get; set; }
        public string MsgId { get; set; }
        public string FileName { get; set; }
        public string FilePath { get; set; }
        public string Tag { get; set; } // "Sender" 또는 "Receiver" 등 사용자 구분 태그
        public bool IsExists { get; set; }
        public DateTime RegisteredTime { get; set; }
    }

    /// <summary>
    /// 송신/수신 파일이 로컬 디스크에서 지워졌는지 감시하는 독립된 Watch 전용 클래스
    /// (P2PSenderNode / P2PReceiverNode와 분리되어 별도로 파일 목록을 관리하고 이벤트로 알려줍니다.)
    /// [사용법]
    /// 1. P2PFileWatchManager.Instance.OnFileMissing += (s, e) => { ... };
    /// 2. P2PFileWatchManager.Instance.RegisterWatch(chId, msgId, filePath);
    /// 3. P2PFileWatchManager.Instance.UnregisterWatch(chId, msgId, filePath);
    /// </summary>
    public class P2PFileWatchManager : IDisposable
    {
        /// <summary>
        /// 어디서든 접근 가능한 글로벌 싱글톤 인스턴스
        /// </summary>
        public static P2PFileWatchManager Instance { get; } = new P2PFileWatchManager();

        // 감시 중인 파일 목록 (Key: "{chId}_{msgId}_{normalizedFilePath}")
        private readonly ConcurrentDictionary<string, WatchedFileItem> _watchedFiles =
            new ConcurrentDictionary<string, WatchedFileItem>(StringComparer.OrdinalIgnoreCase);

        // 폴더별 FileSystemWatcher 관리 (Key: DirectoryPath)
        private readonly ConcurrentDictionary<string, FileSystemWatcher> _dirWatchers =
            new ConcurrentDictionary<string, FileSystemWatcher>(StringComparer.OrdinalIgnoreCase);

        private readonly Timer _pollTimer;
        private readonly object _checkLock = new object();

        /// <summary>
        /// 단순 로그 출력 이벤트
        /// </summary>
        public event Action<string> OnLog;

        /// <summary>
        /// 등록된 파일이 디스크에서 삭제되거나 이동되어 없어졌을 때 발생하는 이벤트
        /// </summary>
        public event EventHandler<FileWatchEventArgs> OnFileMissing;

        /// <summary>
        /// 삭제되었던 파일이 다시 원래 경로에 복구되었을 때 발생하는 이벤트
        /// </summary>
        public event EventHandler<FileWatchEventArgs> OnFileRestored;

        public P2PFileWatchManager()
        {
            // FileSystemWatcher와 함께 상위 폴더 삭제 등도 놓치지 않도록 1초 주기 백그라운드 체크 병행
            _pollTimer = new Timer(_ => CheckAllWatchedFiles(), null, 1000, 1000);
        }

        public static string NormalizePath(string path)
        {
            if (string.IsNullOrWhiteSpace(path)) return string.Empty;
            try
            {
                return Path.GetFullPath(path.Trim().Trim('"'));
            }
            catch
            {
                return path.Trim().Trim('"');
            }
        }

        public static string BuildWatchKey(string chId, string msgId, string filePath)
        {
            string normPath = NormalizePath(filePath);
            return $"{(chId ?? "").Trim()}_{(msgId ?? "").Trim()}_{normPath}";
        }

        /// <summary>
        /// 감시할 파일(chId, msgId, filePath)을 Watch 목록에 등록합니다.
        /// (chId, msgId, filePath)가 모두 동일한 중복 아이템은 중복 등록되지 않고 무시됩니다.
        /// </summary>
        public bool RegisterWatch(string chId, string msgId, string filePath, string tag = null)
        {
            if (string.IsNullOrWhiteSpace(chId)) chId = "DEFAULT_CH";
            if (string.IsNullOrWhiteSpace(msgId) || string.IsNullOrWhiteSpace(filePath))
            {
                Log("[Watch 등록 실패] msgID 또는 파일 경로가 비어 있습니다.");
                return false;
            }

            string normPath = NormalizePath(filePath);
            string key = BuildWatchKey(chId, msgId, normPath);

            // chId, msgId, path가 모두 동일한 중복 아이템 체크
            if (_watchedFiles.ContainsKey(key))
            {
                Log($"[Watch 중복 무시] 이미 동일한 감시 항목이 등록되어 있습니다. (chID:'{chId}', msgID:'{msgId}', 경로:'{normPath}')");
                return false;
            }

            bool currentlyExists = File.Exists(normPath);
            var item = new WatchedFileItem
            {
                WatchKey = key,
                ChId = chId.Trim(),
                MsgId = msgId.Trim(),
                FileName = Path.GetFileName(normPath),
                FilePath = normPath,
                Tag = tag ?? "",
                IsExists = currentlyExists,
                RegisteredTime = DateTime.Now
            };

            if (!_watchedFiles.TryAdd(key, item))
            {
                return false;
            }

            string dir = Path.GetDirectoryName(normPath);
            EnsureDirectoryWatcher(dir);

            Log($"[Watch 등록 완료] chID='{item.ChId}', msgID='{item.MsgId}', 파일='{item.FileName}' (현재존재:{currentlyExists}, 총 감시수:{_watchedFiles.Count}개)");

            if (!currentlyExists)
            {
                RaiseFileMissing(item);
            }

            return true;
        }

        /// <summary>
        /// 감시 목록에서 특정 (chId, msgId, filePath) 파일을 등록 해제합니다.
        /// filePath를 생략(null)하면 해당 (chId, msgId)에 해당하는 모든 감시 항목을 해제합니다.
        /// </summary>
        public bool UnregisterWatch(string chId, string msgId, string filePath = null)
        {
            if (string.IsNullOrWhiteSpace(chId)) chId = "DEFAULT_CH";
            if (string.IsNullOrWhiteSpace(msgId)) return false;

            if (!string.IsNullOrWhiteSpace(filePath))
            {
                string key = BuildWatchKey(chId, msgId, filePath);
                if (_watchedFiles.TryRemove(key, out var removed))
                {
                    Log($"[Watch 해제 완료] chID='{removed.ChId}', msgID='{removed.MsgId}', 파일='{removed.FileName}' 감시 목록에서 제거됨.");
                    return true;
                }
                return false;
            }
            else
            {
                var targets = _watchedFiles.Values
                    .Where(x => x.ChId.Equals(chId.Trim(), StringComparison.OrdinalIgnoreCase) &&
                                x.MsgId.Equals(msgId.Trim(), StringComparison.OrdinalIgnoreCase))
                    .ToList();

                int count = 0;
                foreach (var t in targets)
                {
                    if (_watchedFiles.TryRemove(t.WatchKey, out _))
                    {
                        count++;
                        Log($"[Watch 해제 완료] chID='{t.ChId}', msgID='{t.MsgId}', 파일='{t.FileName}' 감시 목록에서 제거됨.");
                    }
                }
                return count > 0;
            }
        }

        /// <summary>
        /// 모든 감시 목록을 초기화합니다.
        /// </summary>
        public void ClearAllWatches()
        {
            _watchedFiles.Clear();
            foreach (var kv in _dirWatchers)
            {
                try { kv.Value?.Dispose(); } catch { }
            }
            _dirWatchers.Clear();
            Log("[Watch 전체 해제] 모든 파일 감시 목록이 초기화되었습니다.");
        }

        /// <summary>
        /// 현재 감시 중인 모든 파일 목록과 각 파일의 존재 여부(IsExists)를 조회합니다.
        /// </summary>
        public List<WatchedFileItem> GetWatchedFileList(string chId = null)
        {
            CheckAllWatchedFiles();
            var query = _watchedFiles.Values.AsEnumerable();
            if (!string.IsNullOrWhiteSpace(chId))
            {
                query = query.Where(x => x.ChId.Equals(chId.Trim(), StringComparison.OrdinalIgnoreCase));
            }
            return query.OrderBy(x => x.RegisteredTime).ToList();
        }

        private void EnsureDirectoryWatcher(string directoryPath)
        {
            if (string.IsNullOrWhiteSpace(directoryPath) || !Directory.Exists(directoryPath))
                return;

            _dirWatchers.GetOrAdd(directoryPath, dir =>
            {
                try
                {
                    var watcher = new FileSystemWatcher(dir)
                    {
                        NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite | NotifyFilters.Size,
                        IncludeSubdirectories = false,
                        EnableRaisingEvents = true
                    };
                    watcher.Deleted += (s, e) => CheckAllWatchedFiles();
                    watcher.Renamed += (s, e) => CheckAllWatchedFiles();
                    watcher.Created += (s, e) => CheckAllWatchedFiles();
                    return watcher;
                }
                catch
                {
                    return null;
                }
            });
        }

        /// <summary>
        /// 등록된 모든 감시 파일들의 디스크 존재 여부를 즉시 확인하고 변화가 있으면 이벤트를 발생시킵니다.
        /// </summary>
        public void CheckAllWatchedFiles()
        {
            if (!Monitor.TryEnter(_checkLock)) return;
            try
            {
                foreach (var item in _watchedFiles.Values)
                {
                    bool existsNow = File.Exists(item.FilePath);

                    // 있다가 삭제/이동된 경우
                    if (item.IsExists && !existsNow)
                    {
                        item.IsExists = false;
                        RaiseFileMissing(item);
                    }
                    // 없다가 새로 생성/복구된 경우
                    else if (!item.IsExists && existsNow)
                    {
                        item.IsExists = true;
                        RaiseFileRestored(item);
                    }
                }
            }
            catch { }
            finally
            {
                Monitor.Exit(_checkLock);
            }
        }

        private void RaiseFileMissing(WatchedFileItem item)
        {
            string msg = $"감시 중인 파일('{item.FileName}')이 디스크에서 삭제되었거나 이동되었습니다! (chID:'{item.ChId}', msgID:'{item.MsgId}', 경로:'{item.FilePath}')";
            Log($"[파일 삭제 감지 - Watch] {msg}");

            OnFileMissing?.Invoke(this, new FileWatchEventArgs
            {
                ChId = item.ChId,
                MsgId = item.MsgId,
                FileName = item.FileName,
                MissingFilePath = item.FilePath,
                Status = TransferStatus.FileMissing,
                Message = msg
            });
        }

        private void RaiseFileRestored(WatchedFileItem item)
        {
            string msg = $"감시 중인 파일('{item.FileName}')이 디스크에 다시 생성/복구되었습니다. (chID:'{item.ChId}', msgID:'{item.MsgId}', 경로:'{item.FilePath}')";
            Log($"[파일 복구 감지 - Watch] {msg}");

            OnFileRestored?.Invoke(this, new FileWatchEventArgs
            {
                ChId = item.ChId,
                MsgId = item.MsgId,
                FileName = item.FileName,
                MissingFilePath = item.FilePath,
                Status = TransferStatus.Ready,
                Message = msg
            });
        }

        private void Log(string msg)
        {
            OnLog?.Invoke($"[{DateTime.Now:HH:mm:ss.fff}] {msg}");
        }

        public void Dispose()
        {
            try { _pollTimer?.Dispose(); } catch { }
            ClearAllWatches();
        }
    }
}
