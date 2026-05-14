"""
StatsCollector：事件收集器

- 内存队列（deque）接收 track() 调用（O(1)，无 I/O）
- 每 10 秒或满 50 条时批量写入 SQLite（WAL 模式）
- 本地留存，不上传任何数据到外部服务
- 所有异常均被捕获，绝不影响主流程
"""

import logging
import os
import sqlite3
import threading
import time
from collections import deque
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

from .machine import get_machine_id, get_machine_info

# SQLite 数据库路径（持久化，不受 /tmp 清理影响）
_DB_DIR = Path.home() / ".remote-claude"
_DB_PATH = _DB_DIR / "stats.db"
_OLD_DB_DIR = Path.home() / ".local" / "share" / "remote-claude"
_OLD_DB_PATH = _OLD_DB_DIR / "stats.db"

# 兼容迁移：旧 DB 存在而新路径不存在时，自动迁移
if not _DB_PATH.exists() and _OLD_DB_PATH.exists():
    try:
        import shutil as _shutil
        _DB_DIR.mkdir(parents=True, exist_ok=True)
        _shutil.move(str(_OLD_DB_PATH), str(_DB_PATH))
    except Exception:
        pass

# 批量写入阈值
_FLUSH_INTERVAL = 10.0   # 秒
_FLUSH_BATCH = 50        # 条

# 数据保留天数
_EVENTS_RETENTION = 90   # 天
_SUMMARY_RETENTION = 365 # 天


class StatsCollector:
    """事件收集器：内存队列 + SQLite 批量写入"""

    def __init__(self):
        self._queue: deque = deque(maxlen=10000)
        self._lock = threading.Lock()
        self._machine_id = get_machine_id()
        self._conn: Optional[sqlite3.Connection] = None
        self._last_flush = 0.0

        self._init_db()
        # 后台线程定时 flush
        t = threading.Thread(target=self._flush_loop, daemon=True)
        t.start()

    # ── 公开接口 ──────────────────────────────────────────────────────────────

    def track(self, category: str, event: str, session_name: str = '',
               chat_id: str = '', value: int = 1, detail: str = '') -> None:
        """记录事件到本地（非阻塞，线程安全）"""
        try:
            now = time.time()
            date = time.strftime('%Y-%m-%d', time.localtime(now))
            # chat_id 脱敏：只保留前 8 位
            safe_chat_id = chat_id[:8] if chat_id else ''
            row = (now, date, category, event, session_name,
                   safe_chat_id, value, detail, self._machine_id)
            with self._lock:
                self._queue.append(row)
                should_flush = len(self._queue) >= _FLUSH_BATCH
            if should_flush:
                threading.Thread(target=self._flush, daemon=True).start()
        except Exception:
            pass

    def close(self) -> None:
        """关闭前刷新队列"""
        try:
            self._flush()
        except Exception:
            pass

    # ── 内部方法 ──────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        """初始化 SQLite 数据库"""
        try:
            _DB_DIR.mkdir(parents=True, exist_ok=True)
            conn = self._get_conn()
            conn.executescript("""
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL NOT NULL,
                    date TEXT NOT NULL,
                    category TEXT NOT NULL,
                    event TEXT NOT NULL,
                    session_name TEXT DEFAULT '',
                    chat_id TEXT DEFAULT '',
                    value INTEGER DEFAULT 1,
                    detail TEXT DEFAULT '',
                    machine_id TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS daily_summary (
                    date TEXT,
                    category TEXT,
                    event TEXT,
                    count INTEGER,
                    total_value INTEGER,
                    PRIMARY KEY (date, category, event)
                );

                CREATE TABLE IF NOT EXISTS meta (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_events_date
                    ON events(date);
                CREATE INDEX IF NOT EXISTS idx_events_category
                    ON events(category, event);
            """)
            conn.commit()
            # 清理过期数据
            self._cleanup_old_data(conn)
        except Exception:
            pass

    def _get_conn(self) -> sqlite3.Connection:
        """获取 SQLite 连接（线程本地）"""
        if self._conn is None:
            self._conn = sqlite3.connect(str(_DB_PATH), check_same_thread=False)
        return self._conn

    def _flush(self) -> None:
        """批量写入 SQLite"""
        with self._lock:
            if not self._queue:
                return
            rows = list(self._queue)
            self._queue.clear()

        try:
            conn = self._get_conn()
            conn.executemany(
                "INSERT INTO events "
                "(timestamp, date, category, event, session_name, "
                "chat_id, value, detail, machine_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                rows
            )
            conn.commit()
            self._last_flush = time.time()
        except Exception:
            # 写失败，把数据放回队列头部（尽力保留）
            with self._lock:
                for row in reversed(rows):
                    self._queue.appendleft(row)

    def _flush_loop(self) -> None:
        """后台定时 flush 线程"""
        while True:
            try:
                time.sleep(10)
                elapsed = time.time() - self._last_flush
                with self._lock:
                    has_data = bool(self._queue)
                if has_data and elapsed >= _FLUSH_INTERVAL:
                    self._flush()
            except Exception:
                pass

    def _cleanup_old_data(self, conn: sqlite3.Connection) -> None:
        """清理过期数据"""
        try:
            conn.execute(
                "DELETE FROM events WHERE date < date('now', ?)",
                (f"-{_EVENTS_RETENTION} days",)
            )
            conn.execute(
                "DELETE FROM daily_summary WHERE date < date('now', ?)",
                (f"-{_SUMMARY_RETENTION} days",)
            )
            conn.commit()
        except Exception:
            pass
