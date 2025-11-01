import json
from datetime import datetime, timezone
from pathlib import Path
import csv
from typing import Any, Callable, Optional

import oschmod

from console_utils import get_messenger
from custom_logging import BackupCatalog, BackupLogger

class BackupOrchestrationMixin:
    """Handles the high-level logic for creating and managing backups."""

    _messenger : Any
    _logger: BackupLogger
    _database: str
    _database_version : str
    _utility_version : str

    database_schema: Callable[[Path], Optional[str]]
    get_tables: Callable[[], list]
    compress_backup: Callable[[Any], Any]
    table_exists: Callable[..., bool]

    def _create_backup_structure(self, base_path: Path, backup_id: str, back_up_time=None) -> dict:
        backup_root = base_path / backup_id
        data_dir = backup_root / "data"
        backup_diff_dir = backup_root / ".backup_diff"

        backup_root.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)
        backup_diff_dir.mkdir(parents=True, exist_ok=True)

        oschmod.set_mode(backup_diff_dir, "700")

        manifest_path = backup_diff_dir / "manifest.json"
        manifest_data = {
            "base_backup": back_up_time if back_up_time else datetime.now(timezone.utc).isoformat(),
            "diff_chain": [],
            "last_diff_timestamp": back_up_time if back_up_time else datetime.now(timezone.utc).isoformat()
        }
        with open(manifest_path, "w", encoding="utf-8") as manifest_file:
            json.dump(manifest_data, manifest_file, indent=4, ensure_ascii=False)

        oschmod.set_mode(manifest_path, "600")

        return {
            "backup_root": backup_root,
            "root": backup_root,
            "data": data_dir,
            "schema": backup_root / "schema.sql",
            "metadata": backup_root / "metadata.json",
            "diff_root": backup_diff_dir,
            "manifest": manifest_path
        }

    def _prepare_output_directory(self, outpath: Path) -> bool:
        try:
            outpath.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            self._messenger.error(f"Failed to create {outpath}: {e}")
            self._logger.error(f"Dir creation failed: {e}")
            return False

    def _write_table_to_csv(self, file_path: Path, columns: list, rows: list):
        try:
            with file_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
        except Exception as e:
            self._messenger.error(f"Failed to write CSV: {e}")
            self._logger.error(f"CSV write failed: {e}")

    def _log_table_backup(self, metadata: dict, table_name: str, rows_count: int, file_size: int, file_path: str):
        if metadata:
            self._logger.log_table_backup(
                metadata=metadata,
                table_name=table_name,
                rows_count=rows_count,
                file_size=file_size,
                file_path=file_path
            )

    def _save_metadata(self, metadata: dict, filepath: Path):
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=4, ensure_ascii=False)
            self._messenger.success(f"Metadata saved: {filepath}")
        except Exception as e:
            self._messenger.error(f"Failed to save metadata: {e}")
            self._logger.error(f"Metadata save failed: {e}")
    
    def export_table(self, tables, outpath, metadata=None) -> list[dict]:
        saved_files = []
        outpath = Path(outpath) if isinstance(outpath, str) else outpath
        if not self._prepare_output_directory(outpath):
            return []

        for schema, table_name in tables:
            file_path = outpath / f"{table_name}.csv"
            export_result = self._export_single_table(schema, table_name, file_path, metadata)
            if export_result:
                saved_files.append(export_result)

        return saved_files
