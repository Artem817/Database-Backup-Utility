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

    _export_single_table: Callable[..., Optional[dict]]

    def backup_full(self, outpath, export_type: str = "csv", compress: bool = False):
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        self._messenger.info(f"Starting full backup → {base_path}")
        if compress:
            self._messenger.info("Compression enabled")

        metadata = self._logger.start_backup(
            backup_type="full",
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version=self._utility_version,
            compress=compress
        )

        try:
            backup_structure = self._create_backup_structure(base_path, metadata["id"],
                                                             back_up_time=metadata["timestamp_start"])
            self._messenger.info(f"Backup dir: {backup_structure['root']}")

            schema_path = self.database_schema(backup_structure["schema"])
            if (schema_path):
                metadata["schema_file"] = str(backup_structure["schema"])

            tables = self.get_tables()

            if not tables:
                self._messenger.warning("No tables found")
                self._logger.warning("No tables for backup")
                self._logger.finish_backup(metadata, success=False)
                return False

            self._messenger.info(f"Found {len(tables)} table(s)...")
            export = self.export_table(tables, backup_structure["data"], metadata=metadata)
            if not export:
                self._messenger.error("Backup failed - no files exported")
                self._logger.finish_backup(metadata, success=False)
                return False

            metadata["backup_location"] = str(backup_structure["root"])
            self._logger.finish_backup(metadata, success=True)
            self._save_metadata(metadata, backup_structure["metadata"])

            if compress:
                self._messenger.info("Compressing...")
                self.compress_backup(backup_structure['root'])

            self._messenger.success("Full backup completed")
            return True

        except Exception as e:
            self._messenger.error(f"Backup failed: {e}")
            self._logger.error(f"Backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False

    def partial_backup(self, tables: list, outpath: str, backup_type: str = "partial", compress: bool = False):
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        self._messenger.info(f"Starting {backup_type} backup → {base_path}")
        if compress:
            self._messenger.info("Compression enabled")

        metadata = self._logger.start_backup(
            backup_type=backup_type,
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version=self._utility_version,
            compress=compress
        )

        try:
            backup_structure = self._create_backup_structure(base_path, metadata["id"], back_up_time=metadata["timestamp_start"])
            self._messenger.info(f"Backup dir: {backup_structure['root']}")

            schema_path = self.database_schema(backup_structure["schema"])
            if schema_path:
                metadata["schema_file"] = str(backup_structure["schema"])

            verified_tables = []
            for table in tables:
                if self.table_exists(table_name=table):
                    verified_tables.append(("public", table))
                    self._messenger.success(f"Table '{table}' found")
                    self._logger.info(f"Table '{table}' verified")
                else:
                    self._messenger.error(f"Table '{table}' doesn't exist")
                    self._logger.warning(f"Table '{table}' missing")

            if not verified_tables:
                self._messenger.warning("No valid tables to export")
                self._logger.finish_backup(metadata, success=False)
                return False

            export = self.export_table(verified_tables, backup_structure["data"], metadata=metadata)
            if not export:
                self._messenger.error("Backup failed - no files exported")
                self._logger.finish_backup(metadata, success=False)
                return False

            metadata["backup_location"] = str(backup_structure["root"])
            self._logger.finish_backup(metadata, success=True)
            self._save_metadata(metadata, backup_structure["metadata"])

            if compress:
                self._messenger.info("Compressing...")
                self.compress_backup(backup_structure['root'])

            self._messenger.success("Partial backup completed")
            return True
        except Exception as e:
            self._messenger.error(f"Backup failed: {e}")
            self._logger.error(f"Partial backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False

    def _create_backup_structure(self, base_path: Path, backup_id: str, back_up_time=None) -> dict:
        backup_root = base_path / backup_id
        data_dir = backup_root / "data"
        backup_diff_dir = backup_root / ".backup_diff"

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