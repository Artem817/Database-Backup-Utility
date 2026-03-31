import json
from pathlib import Path
from typing import Dict, Any


class IncrementalMetadataWriter:
    """
    Writes metadata.json for the entire incremental backup.
    """

    def __init__(self, logger, messenger=None):
        self._logger = logger
        self._messenger = messenger

    def execute(self, ctx) -> bool:
        # ctx — IncrementalBackupContext (collector)
        metadata: Dict[str, Any] = {
            "backup_type": "incremental",
            "backup_location": str(ctx.backup_dir),
            "parent_backup_location": ctx.parent_metadata.get("backup_location"),
            "parent_backup_id": ctx.parent_metadata.get("id"),

            "start_lsn": ctx.start_lsn,
            "end_lsn": ctx.end_lsn,
            "previous_wal_file": ctx.previous_wal_file,
            "current_wal_file": ctx.current_wal_file,

            "wal_archive_directory": str(ctx.archive_dir),
            "wal_files_count": len(ctx.wal_metadata_items or []),
            "wal_files": ctx.wal_metadata_items or [],

            "backup_size_bytes": sum(i["size_bytes"] for i in (ctx.wal_metadata_items or [])),
            "mode": "wal_based_incremental",
        }

        try:
            out = Path(ctx.backup_dir) / "metadata.json"
            with out.open("w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)

            self._logger.info(f"Incremental metadata saved: {out}")
            if self._messenger:
                self._messenger.info(f"Metadata saved: {out.name}")
            return True
        except Exception as e:
            self._logger.error(f"Failed to write incremental metadata: {e}", exc_info=True)
            if self._messenger:
                self._messenger.error(f"Failed to write incremental metadata: {e}")
            raise
