import logging
from logging import getLoggerClass
from typing import Any, Dict, Optional
import uuid
from datetime import datetime, timezone

from services.backup.catalog import BackupCatalog as SQLiteBackupCatalog
from services.backup.contracts import BackupContractManager

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

        self.catalog = SQLiteBackupCatalog()
        self._contract_manager = BackupContractManager(self)

    def start_backup(
        self,
        backup_type: str,
        database: str,
        storage: str,
        database_version: str,
        utility_version: str,
        compress: bool,
        database_type: str = "unknown",
    ) -> Dict[str, Any]:
        timestamp_start = datetime.now(timezone.utc)
        backup_id = generate_backup_id(backup_type, database, timestamp_start)
        parent_backup_id = None
        base_backup_id = None

        if backup_type == "incremental":
            last_backup = self.catalog.get_last_successful_backup(database_name=database)
            parent_backup_id = last_backup.get("id") if last_backup else None
        elif backup_type == "differential":
            last_full = self.catalog.get_last_full_backup(database_name=database)
            base_backup_id = last_full.get("id") if last_full else None
        
        metadata = {
            "id": backup_id,
            "type": backup_type,
            "version": utility_version,
            "database_type": database_type,
            "database_version": database_version,
            "database_name": database,
            "backup_manifest_path" :"",
            "timestamp_start": timestamp_start.isoformat(),
            "timestamp_end": None,
            "duration_seconds": None,
            "parent_backup_id": parent_backup_id,
            "base_backup_id": base_backup_id,
            "compress": compress,
            "storage": storage,
            "compress_format": None,
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
        timestamp_end = datetime.now(timezone.utc)
        timestamp_start = datetime.fromisoformat(metadata["timestamp_start"])
        duration = (timestamp_end - timestamp_start).total_seconds()

        metadata["timestamp_end"] = timestamp_end.isoformat()
        metadata["duration_seconds"] = duration
        metadata["status"] = "completed" if success else "failed"

        if metadata["statistics"].get("total_size_bytes", 0) == 0:
            backup_size = metadata.get("backup_size_bytes")
            if isinstance(backup_size, int):
                metadata["statistics"]["total_size_bytes"] = backup_size

        backup_location = metadata.get("backup_location")
        if backup_location:
            try:
                contract_metadata = self._contract_manager.persist_backup_documents(
                    metadata=metadata,
                    output_dir=backup_location,
                    backup_succeeded=success,
                )
                metadata.clear()
                metadata.update(contract_metadata)
                success = metadata.get("status") == "completed"
            except Exception as error:
                metadata["status"] = "failed"
                success = False
                self.logger.error(f"Failed to persist backup contract: {error}", exc_info=True)

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

class BackupCatalog(SQLiteBackupCatalog):
    pass
