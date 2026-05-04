from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, Optional


CATALOG_SCHEMA_VERSION = "1.0"


class BackupCatalog:
    def __init__(
        self,
        path: str = "backup_catalog.db",
        legacy_json_path: str = "backup_catalog.json",
    ):
        if not isinstance(path, str):
            raise ValueError("The catalog path must be a string.")
        if not path.endswith(".db"):
            raise ValueError("The catalog file must be a SQLite .db file.")

        self.catalog_path = Path(path)
        self.legacy_json_path = Path(legacy_json_path)
        self.logger = logging.getLogger("BackupCatalog")

        self._initialize_schema()
        self._import_legacy_json_if_needed()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.catalog_path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize_schema(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS catalog_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS backups (
                    backup_id TEXT PRIMARY KEY,
                    database_name TEXT NOT NULL,
                    engine TEXT NOT NULL,
                    backup_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    timestamp_start TEXT NOT NULL,
                    timestamp_end TEXT,
                    duration_seconds REAL,
                    parent_backup_id TEXT,
                    base_backup_id TEXT,
                    backup_location TEXT,
                    metadata_path TEXT,
                    validation_path TEXT,
                    current_lsn TEXT,
                    start_lsn TEXT,
                    end_lsn TEXT,
                    current_wal_file TEXT,
                    previous_wal_file TEXT,
                    last_backup_wal_file TEXT,
                    binlog_file TEXT,
                    binlog_position INTEGER,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_backups_lookup
                ON backups (database_name, status, backup_type, timestamp_start DESC)
                """
            )
            connection.execute(
                """
                INSERT OR REPLACE INTO catalog_meta (key, value)
                VALUES ('schema_version', ?)
                """,
                (CATALOG_SCHEMA_VERSION,),
            )

    def _import_legacy_json_if_needed(self) -> None:
        if not self.legacy_json_path.exists():
            return

        if self._has_backups():
            return

        try:
            with self.legacy_json_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (json.JSONDecodeError, OSError) as error:
            self.logger.error("Failed to import legacy catalog JSON: %s", error)
            return

        backups = payload.get("backups", [])
        if not isinstance(backups, list):
            self.logger.error("Legacy catalog JSON is malformed: 'backups' must be a list.")
            return

        for backup in backups:
            if isinstance(backup, dict):
                self.add_backup(backup)

        self.logger.info(
            "Imported %s backup records from legacy catalog %s",
            len(backups),
            self.legacy_json_path,
        )

    def _has_backups(self) -> bool:
        with self._connect() as connection:
            row = connection.execute("SELECT COUNT(*) AS count FROM backups").fetchone()
            return bool(row["count"])

    def add_backup(self, new_backup: Dict[str, Any]) -> None:
        if not isinstance(new_backup, dict):
            raise ValueError("The new backup must be a dictionary.")

        payload = dict(new_backup)
        lsn_markers = payload.get("lsn_markers", {}) if isinstance(payload.get("lsn_markers"), dict) else {}
        binlog_markers = (
            payload.get("binlog_markers", {})
            if isinstance(payload.get("binlog_markers"), dict)
            else {}
        )

        backup_id = payload.get("id") or payload.get("backup_id")
        if not backup_id:
            raise ValueError("Backup metadata must contain id or backup_id.")

        record = {
            "backup_id": backup_id,
            "database_name": payload.get("database_name") or "",
            "engine": payload.get("engine") or payload.get("database_type") or "unknown",
            "backup_type": payload.get("backup_type") or payload.get("type") or "unknown",
            "status": payload.get("status") or "unknown",
            "timestamp_start": payload.get("timestamp_start") or "",
            "timestamp_end": payload.get("timestamp_end"),
            "duration_seconds": payload.get("duration_seconds"),
            "parent_backup_id": payload.get("parent_backup_id"),
            "base_backup_id": payload.get("base_backup_id"),
            "backup_location": payload.get("backup_location"),
            "metadata_path": payload.get("metadata_path"),
            "validation_path": payload.get("validation_path"),
            "current_lsn": lsn_markers.get("current_lsn") or payload.get("current_lsn"),
            "start_lsn": lsn_markers.get("start_lsn") or payload.get("start_lsn"),
            "end_lsn": lsn_markers.get("end_lsn") or payload.get("end_lsn"),
            "current_wal_file": lsn_markers.get("current_wal_file") or payload.get("current_wal_file"),
            "previous_wal_file": lsn_markers.get("previous_wal_file") or payload.get("previous_wal_file"),
            "last_backup_wal_file": (
                lsn_markers.get("last_backup_wal_file") or payload.get("last_backup_wal_file")
            ),
            "binlog_file": binlog_markers.get("binlog_file") or payload.get("binlog_file"),
            "binlog_position": (
                binlog_markers.get("binlog_position") if binlog_markers.get("binlog_position") is not None
                else payload.get("binlog_position")
            ),
            "payload_json": json.dumps(payload, ensure_ascii=False, default=str),
        }

        with self._connect() as connection:
            connection.execute(
                """
                INSERT OR REPLACE INTO backups (
                    backup_id,
                    database_name,
                    engine,
                    backup_type,
                    status,
                    timestamp_start,
                    timestamp_end,
                    duration_seconds,
                    parent_backup_id,
                    base_backup_id,
                    backup_location,
                    metadata_path,
                    validation_path,
                    current_lsn,
                    start_lsn,
                    end_lsn,
                    current_wal_file,
                    previous_wal_file,
                    last_backup_wal_file,
                    binlog_file,
                    binlog_position,
                    payload_json
                ) VALUES (
                    :backup_id,
                    :database_name,
                    :engine,
                    :backup_type,
                    :status,
                    :timestamp_start,
                    :timestamp_end,
                    :duration_seconds,
                    :parent_backup_id,
                    :base_backup_id,
                    :backup_location,
                    :metadata_path,
                    :validation_path,
                    :current_lsn,
                    :start_lsn,
                    :end_lsn,
                    :current_wal_file,
                    :previous_wal_file,
                    :last_backup_wal_file,
                    :binlog_file,
                    :binlog_position,
                    :payload_json
                )
                """,
                record,
            )

    def list_backups(
        self,
        database_name: str | None = None,
        status: str | None = None,
        backup_type: str | None = None,
        limit: int | None = None,
    ) -> list[Dict[str, Any]]:
        query = "SELECT payload_json FROM backups"
        clauses: list[str] = []
        params: list[Any] = []

        if database_name:
            clauses.append("database_name = ?")
            params.append(database_name)
        if status:
            clauses.append("status = ?")
            params.append(status)
        if backup_type:
            clauses.append("backup_type = ?")
            params.append(backup_type)

        if clauses:
            query += " WHERE " + " AND ".join(clauses)

        query += " ORDER BY timestamp_start DESC"
        if limit is not None:
            query += " LIMIT ?"
            params.append(limit)

        with self._connect() as connection:
            rows = connection.execute(query, params).fetchall()
        return [json.loads(row["payload_json"]) for row in rows]

    def get_backup_by_id(self, backup_id: str) -> Optional[Dict[str, Any]]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT payload_json FROM backups WHERE backup_id = ?",
                (backup_id,),
            ).fetchone()

        if not row:
            return None
        return json.loads(row["payload_json"])

    def get_last_backup(self, database_name: str | None = None) -> Optional[Dict[str, Any]]:
        backups = self.list_backups(database_name=database_name, limit=1)
        return backups[0] if backups else None

    def get_last_successful_backup(self, database_name: str | None = None) -> Optional[Dict[str, Any]]:
        backups = self.list_backups(database_name=database_name, status="completed", limit=1)
        return backups[0] if backups else None

    def get_last_backup_id(self, database_name: str | None = None) -> Optional[str]:
        last = self.get_last_backup(database_name=database_name)
        return last.get("id") if last else None

    def get_last_backup_by_type(
        self,
        backup_type: str,
        database_name: str | None = None,
    ) -> Optional[Dict[str, Any]]:
        if not isinstance(backup_type, str):
            raise ValueError("The backup type must be a string.")

        backups = self.list_backups(
            database_name=database_name,
            status="completed",
            backup_type=backup_type,
            limit=1,
        )
        return backups[0] if backups else None

    def get_last_full_backup(self, database_name: str | None = None) -> Optional[Dict[str, Any]]:
        return self.get_last_backup_by_type("full", database_name=database_name)

    def get_backup_chain(self, backup_id: str) -> list[Dict[str, Any]]:
        target = self.get_backup_by_id(backup_id)
        if not target:
            return []

        chain = [target]
        current = target
        database_name = target.get("database_name")

        while current.get("parent_backup_id") or current.get("base_backup_id"):
            parent_id = current.get("parent_backup_id") or current.get("base_backup_id")
            parent = self.get_backup_by_id(parent_id)
            if not parent:
                break
            if database_name and parent.get("database_name") != database_name:
                self.logger.error(
                    "Backup chain crosses database boundary: %s -> %s",
                    current.get("id"),
                    parent.get("id"),
                )
                break
            chain.insert(0, parent)
            current = parent

        return chain
