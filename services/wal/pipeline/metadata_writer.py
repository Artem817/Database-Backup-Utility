from pathlib import Path
from typing import Dict, Any
from datetime import datetime, timezone

from services.backup.contracts import BackupContractManager


class IncrementalMetadataWriter:
    """
    Writes metadata.json for the entire incremental backup.
    """

    def __init__(self, logger, messenger=None):
        self._logger = logger
        self._messenger = messenger
        self._contract_manager = BackupContractManager(logger, messenger)

    def execute(self, ctx) -> bool:
        # ctx — IncrementalBackupContext (collector)
        timestamp = datetime.now(timezone.utc).isoformat()
        metadata: Dict[str, Any] = {
            "schema_version": "1.0",
            "id": Path(ctx.backup_dir).name,
            "backup_type": "incremental",
            "type": "incremental",
            "engine": ctx.parent_metadata.get("engine") or ctx.parent_metadata.get("database_type") or "postgresql",
            "database_type": ctx.parent_metadata.get("engine") or ctx.parent_metadata.get("database_type") or "postgresql",
            "database_name": ctx.parent_metadata.get("database_name"),
            "database_version": ctx.parent_metadata.get("database_version", "unknown"),
            "backup_location": str(ctx.backup_dir),
            "parent_backup_location": ctx.parent_metadata.get("backup_location"),
            "parent_backup_id": ctx.parent_metadata.get("id"),
            "base_backup_id": ctx.parent_metadata.get("base_backup_id") or ctx.parent_metadata.get("id"),
            "storage": ctx.parent_metadata.get("storage", "local"),
            "compress": False,
            "timestamp_start": timestamp,
            "timestamp_end": timestamp,
            "duration_seconds": 0.0,
            "status": "completed",

            "start_lsn": ctx.start_lsn,
            "end_lsn": ctx.end_lsn,
            "previous_wal_file": ctx.previous_wal_file,
            "current_wal_file": ctx.current_wal_file,

            "wal_archive_directory": str(ctx.archive_dir),
            "wal_files_count": len(ctx.wal_metadata_items or []),
            "wal_files": ctx.wal_metadata_items or [],

            "backup_size_bytes": sum(i["size_bytes"] for i in (ctx.wal_metadata_items or [])),
            "mode": "wal_based_incremental",
            "statistics": {
                "total_tables": 0,
                "total_rows_processed": 0,
                "total_size_bytes": sum(i["size_bytes"] for i in (ctx.wal_metadata_items or [])),
            },
        }

        try:
            self._contract_manager.persist_backup_documents(
                metadata=metadata,
                output_dir=ctx.backup_dir,
                backup_succeeded=True,
            )
            out = Path(ctx.backup_dir) / "metadata.json"
            self._logger.info(f"Incremental metadata saved: {out}")
            if self._messenger:
                self._messenger.info(f"Metadata saved: {out.name}")
            return True
        except Exception as e:
            self._logger.error(f"Failed to write incremental metadata: {e}", exc_info=True)
            if self._messenger:
                self._messenger.error(f"Failed to write incremental metadata: {e}")
            raise
