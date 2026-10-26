"""
MeshWeaver Persistent Storage Subsystem
Provides zero-dependency persistence for tasks, audit logs, and cluster telemetry
using Python's built-in sqlite3 with Write-Ahead Logging (WAL) mode.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("MeshWeaver.Storage")

DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "meshweaver.db")


class MeshWeaverStorage:
    """Thread-safe SQLite storage engine for MeshWeaver nodes."""

    _instances: Dict[str, MeshWeaverStorage] = {}
    _lock = threading.Lock()

    def __init__(self, db_path: Optional[str] = None) -> None:
        self.db_path = db_path or DEFAULT_DB_PATH
        db_dir = os.path.dirname(os.path.abspath(self.db_path))
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

        self._local = threading.local()
        self._init_schema()
        logger.info(f"Initialized SQLite storage at {self.db_path}")

    @classmethod
    def get_instance(cls, db_path: Optional[str] = None) -> MeshWeaverStorage:
        """Singleton accessor per database path."""
        path = db_path or DEFAULT_DB_PATH
        with cls._lock:
            if path not in cls._instances:
                cls._instances[path] = cls(path)
            return cls._instances[path]

    def _get_connection(self) -> sqlite3.Connection:
        """Returns a thread-local SQLite connection configured for performance."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            conn = sqlite3.connect(self.db_path, timeout=10.0)
            conn.row_factory = sqlite3.Row
            # Enable WAL mode for high concurrency
            try:
                conn.execute("PRAGMA journal_mode = WAL;")
                conn.execute("PRAGMA synchronous = NORMAL;")
            except Exception:
                pass
            self._local.conn = conn
        return self._local.conn

    def _init_schema(self) -> None:
        """Initializes tables and indexes."""
        conn = self._get_connection()
        with conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS tasks (
                    task_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    priority INTEGER DEFAULT 1,
                    assigned_worker TEXT,
                    execution_time_ms REAL DEFAULT 0.0,
                    result_json TEXT,
                    error_message TEXT,
                    created_at REAL,
                    completed_at REAL
                );

                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    epoch_time REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS node_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT NOT NULL,
                    cpu_percent REAL NOT NULL,
                    ram_percent REAL NOT NULL,
                    active_tasks INTEGER DEFAULT 0,
                    status TEXT NOT NULL,
                    epoch_time REAL NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
                CREATE INDEX IF NOT EXISTS idx_tasks_completed_at ON tasks(completed_at);
                CREATE INDEX IF NOT EXISTS idx_events_epoch ON audit_events(epoch_time);
                CREATE INDEX IF NOT EXISTS idx_snapshots_epoch ON node_snapshots(epoch_time);
            """)

    # -------------------------------------------------------------------------
    # Task Operations
    # -------------------------------------------------------------------------

    def save_task(
        self,
        task_id: str,
        name: str,
        status: str,
        priority: int = 1,
        assigned_worker: Optional[str] = None,
        execution_time_ms: float = 0.0,
        result: Optional[Any] = None,
        error_message: Optional[str] = None,
        created_at: Optional[float] = None,
        completed_at: Optional[float] = None,
    ) -> None:
        """Inserts or updates a task record."""
        result_str = None
        if result is not None:
            try:
                result_str = json.dumps(result)
            except Exception:
                result_str = str(result)

        now = time.time()
        c_at = created_at or now
        comp_at = completed_at or (now if status in ("COMPLETED", "FAILED") else None)

        conn = self._get_connection()
        with conn:
            conn.execute("""
                INSERT INTO tasks (
                    task_id, name, status, priority, assigned_worker,
                    execution_time_ms, result_json, error_message, created_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(task_id) DO UPDATE SET
                    status=excluded.status,
                    assigned_worker=COALESCE(excluded.assigned_worker, tasks.assigned_worker),
                    execution_time_ms=excluded.execution_time_ms,
                    result_json=COALESCE(excluded.result_json, tasks.result_json),
                    error_message=COALESCE(excluded.error_message, tasks.error_message),
                    completed_at=COALESCE(excluded.completed_at, tasks.completed_at);
            """, (
                task_id, name, status, priority, assigned_worker,
                execution_time_ms, result_str, error_message, c_at, comp_at
            ))

    def get_task(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single task by ID."""
        conn = self._get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,))
        row = cur.fetchone()
        if not row:
            return None
        return self._format_task_row(row)

    def get_recent_tasks(self, limit: int = 50, status: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieves recent tasks ordered by completion or creation time."""
        conn = self._get_connection()
        cur = conn.cursor()
        if status:
            cur.execute("""
                SELECT * FROM tasks WHERE status = ?
                ORDER BY COALESCE(completed_at, created_at) DESC LIMIT ?
            """, (status, limit))
        else:
            cur.execute("""
                SELECT * FROM tasks
                ORDER BY COALESCE(completed_at, created_at) DESC LIMIT ?
            """, (limit,))
        return [self._format_task_row(r) for r in cur.fetchall()]

    def count_tasks(self, status: Optional[str] = None) -> int:
        """Returns total count of stored tasks."""
        conn = self._get_connection()
        cur = conn.cursor()
        if status:
            cur.execute("SELECT COUNT(*) FROM tasks WHERE status = ?", (status,))
        else:
            cur.execute("SELECT COUNT(*) FROM tasks")
        return cur.fetchone()[0]

    def _format_task_row(self, row: sqlite3.Row) -> Dict[str, Any]:
        result_parsed = None
        if row["result_json"]:
            try:
                result_parsed = json.loads(row["result_json"])
            except Exception:
                result_parsed = row["result_json"]

        return {
            "task_id": row["task_id"],
            "task_name": row["name"],
            "status": row["status"],
            "priority": row["priority"],
            "worker_node_id": row["assigned_worker"],
            "execution_time_ms": row["execution_time_ms"],
            "result": result_parsed,
            "error_message": row["error_message"],
            "created_at": row["created_at"],
            "completed_at": row["completed_at"],
        }

    # -------------------------------------------------------------------------
    # Audit Event Operations
    # -------------------------------------------------------------------------

    def save_event(self, level: str, message: str, timestamp_str: Optional[str] = None) -> None:
        """Inserts an audit event log."""
        now = time.time()
        ts = timestamp_str or datetime.datetime.fromtimestamp(now).strftime("%H:%M:%S")
        conn = self._get_connection()
        with conn:
            conn.execute("""
                INSERT INTO audit_events (timestamp, level, message, epoch_time)
                VALUES (?, ?, ?, ?)
            """, (ts, level.upper(), message, now))

    def get_recent_events(self, limit: int = 50, level: Optional[str] = None) -> List[Dict[str, Any]]:
        """Retrieves recent audit events."""
        conn = self._get_connection()
        cur = conn.cursor()
        if level:
            cur.execute("""
                SELECT * FROM audit_events WHERE level = ?
                ORDER BY epoch_time DESC, id DESC LIMIT ?
            """, (level.upper(), limit))
        else:
            cur.execute("""
                SELECT * FROM audit_events
                ORDER BY epoch_time DESC, id DESC LIMIT ?
            """, (limit,))
        rows = cur.fetchall()
        # Return chronological order (oldest to newest for terminal display)
        rows.reverse()
        return [
            {"id": r["id"], "timestamp": r["timestamp"], "level": r["level"], "message": r["message"]}
            for r in rows
        ]

    # -------------------------------------------------------------------------
    # Node Telemetry Snapshots
    # -------------------------------------------------------------------------

    def record_node_snapshot(
        self,
        node_id: str,
        cpu_percent: float,
        ram_percent: float,
        active_tasks: int = 0,
        status: str = "HEALTHY",
    ) -> None:
        """Records a time-series telemetry snapshot of a node."""
        conn = self._get_connection()
        with conn:
            conn.execute("""
                INSERT INTO node_snapshots (node_id, cpu_percent, ram_percent, active_tasks, status, epoch_time)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (node_id, cpu_percent, ram_percent, active_tasks, status, time.time()))

    def clear_all(self) -> None:
        """Clears all stored data (useful for test resets)."""
        conn = self._get_connection()
        with conn:
            conn.execute("DELETE FROM tasks;")
            conn.execute("DELETE FROM audit_events;")
            conn.execute("DELETE FROM node_snapshots;")

    def close(self) -> None:
        """Closes thread-local connection."""
        if hasattr(self._local, "conn") and self._local.conn is not None:
            self._local.conn.close()
            self._local.conn = None
