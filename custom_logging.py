import json
import logging
from logging import getLoggerClass
import os
from typing import Any, Dict, Optional
import uuid
from datetime import datetime

OriginalLogger = getLoggerClass()

def generate_backup_id(backup_type: str, database: str, timestamp: datetime) -> str:
    timestamp_format = "%Y%m%d_%H%M%S"
    suffix = uuid.uuid4().hex[:4]
    return f"{backup_type}_{database}_{timestamp.strftime(timestamp_format)}_{suffix}"

class BackupLogger:
    def __init__(self, name: str = "backup", log_file: str = "backup.log", level: int = logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)
        self.logger.handlers.clear()

        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(level)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        self.catalog = BackupCatalog()

    def start_backup(self, backup_type: str, database: str, database_version: str, utility_version: str, compress: bool) -> Dict[str, Any]:
        timestamp_start = datetime.now()
        backup_id = generate_backup_id(backup_type, database, timestamp_start)
        parent_backup_id = None
        base_backup_id = None

        if backup_type == "incremental":
            last_backup = self.catalog.get_last_backup()
            parent_backup_id = last_backup.get("id") if last_backup else None
        elif backup_type == "differential":
            last_full = self.catalog.get_last_full_backup()
            base_backup_id = last_full.get("id") if last_full else None

        metadata = {
            "id": backup_id,
            "type": backup_type,
            "version": utility_version,
            "database_type": "postgresql",
            "database_version": database_version,
            "database_name": database,
            "timestamp_start": timestamp_start.isoformat(),
            "timestamp_end": None,
            "duration_seconds": None,
            "parent_backup_id": parent_backup_id,
            "base_backup_id": base_backup_id,
            "compress": compress,
            "compress_format": "zip",
            "status": "in_progress",
            "tables": {},
            "statistics": {
                "total_tables": 0,
                "total_rows_processed": 0,
                "total_size_bytes": 0,
            },
        }

        self.logger.info(f"Starting {backup_type} backup: {backup_id}")
        if parent_backup_id:
            self.logger.info(f"Parent backup: {parent_backup_id}")
        if base_backup_id:
            self.logger.info(f"Base backup: {base_backup_id}")
        return metadata

    def finish_backup(self, metadata: Dict[str, Any], success: bool = True) -> None:
        timestamp_end = datetime.now()
        timestamp_start = datetime.fromisoformat(metadata["timestamp_start"])
        duration = (timestamp_end - timestamp_start).total_seconds()

        metadata["timestamp_end"] = timestamp_end.isoformat()
        metadata["duration_seconds"] = duration
        metadata["status"] = "completed" if success else "failed"

        self.catalog.add_backup(metadata)

        if success:
            self.logger.info(
                f"Backup completed: {metadata['id']} "
                f"({duration:.2f}s, {metadata['statistics']['total_size_bytes'] / 1024 / 1024:.2f} MB)"
            )
        else:
            self.logger.error(f"Backup failed: {metadata['id']}")

    def log_table_backup(self, metadata: Dict[str, Any], table_name: str, rows_count: int, file_size: int, file_path: str) -> None:
        metadata["tables"][table_name] = {
            "rows_count": rows_count,
            "file_size_bytes": file_size,
            "file_path": file_path,
        }
        metadata["statistics"]["total_tables"] += 1
        metadata["statistics"]["total_rows_processed"] += rows_count
        metadata["statistics"]["total_size_bytes"] += file_size
        self.logger.info(f"Table {table_name}: {rows_count} rows, {file_size / 1024:.2f} KB")

    def info(self, message: str) -> None:
        self.logger.info(message)

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def error(self, message: str) -> None:
        self.logger.error(message)


class BackupCatalog:
    def __init__(self, path: str = "backup_catalog.json"):
        if not isinstance(path, str):
            raise ValueError("The catalog path must be a string.")
        if not path.endswith(".json"):
            raise ValueError("The catalog file must be a JSON file.")
        self.catalog_path = path
        self.catalog = self.load()

    def load(self) -> Dict[str, Any]:
        if not os.path.exists(self.catalog_path):
            return {"backups": []}
        try:
            with open(self.catalog_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            raise ValueError("The catalog file contains invalid JSON.")

    def save(self) -> None:
        if not isinstance(self.catalog, dict):
            raise ValueError("The catalog must be a dictionary.")
        with open(self.catalog_path, "w", encoding="utf-8") as f:
            json.dump(self.catalog, f, indent=4, ensure_ascii=False)

    def add_backup(self, new_backup: Dict[str, Any]) -> None:
        if not isinstance(new_backup, dict):
            raise ValueError("The new backup must be a dictionary.")
        if "backups" not in self.catalog:
            self.catalog["backups"] = []
        self.catalog["backups"].append(new_backup)
        self.save()

    def get_last_backup(self) -> Optional[Dict[str, Any]]:
        backups = self.catalog.get("backups", [])
        if not isinstance(backups, list):
            raise ValueError("The backups must be a list.")
        if not backups:
            return None
        return max(backups, key=lambda b: b.get("timestamp_start", ""))

    def get_last_backup_id(self) -> Optional[str]:
        last = self.get_last_backup()
        return last.get("id") if last else None

    def get_last_backup_by_type(self, backup_type: str) -> Optional[Dict[str, Any]]:
        if not isinstance(backup_type, str):
            raise ValueError("The backup type must be a string.")
        backups = self.catalog.get("backups", [])
        if not isinstance(backups, list):
            raise ValueError("The backups must be a list.")
        filtered = [b for b in backups if b.get("type") == backup_type and b.get("status") == "completed"]
        if not filtered:
            return None
        return max(filtered, key=lambda b: b.get("timestamp_start", ""))

    def get_last_full_backup(self) -> Optional[Dict[str, Any]]:
        return self.get_last_backup_by_type("full")

    def get_backup_chain(self, backup_id: str) -> list[Dict[str, Any]]:
        backups = self.catalog.get("backups", [])
        target = next((b for b in backups if b.get("id") == backup_id), None)
        if not target:
            return []
        chain = [target]
        current = target
        while current.get("parent_backup_id") or current.get("base_backup_id"):
            parent_id = current.get("parent_backup_id") or current.get("base_backup_id")
            parent = next((b for b in backups if b.get("id") == parent_id), None)
            if not parent:
                break
            chain.insert(0, parent)
            current = parent
        return chain
