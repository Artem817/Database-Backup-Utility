import json
import shutil
from datetime import datetime
from pathlib import Path

import oschmod

from services.interfaces import IConnectionProvider, ILogger, IMessenger


class DifferentialBackupService:
    def __init__(self,
                 connection_provider: IConnectionProvider,
                 logger: ILogger,
                 messenger: IMessenger,
                 schema_exporter,
                 table_exporter):
        self._connection_provider = connection_provider
        self._logger = logger
        self._messenger = messenger
        self._schema_exporter = schema_exporter
        self._table_exporter = table_exporter

    def get_max_updated_at(self, table_name: str, schema: str, column: str):
        try:
            query = f'SELECT MAX({column}) FROM "{schema}"."{table_name}"'
            connection = self._connection_provider.get_connection()
            with connection.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()[0]
                return result.isoformat() if result else "None"
        except Exception as e:
            self._logger.error(f"get_max_updated_at failed: {e}")
            connection.rollback()
            return "Error"

    def export_diff_table(self, tables, last_backup_time: datetime, outpath: Path, basis: str) -> dict:
        self._messenger.warning(f"Exporting differential data since {last_backup_time} using '{basis}'")

        outpath = Path(outpath) if isinstance(outpath, str) else outpath
        diff_root = outpath / ".backup_diff"
        diff_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        diff_dir = diff_root / diff_timestamp

        if not self._prepare_output_directory(diff_dir):
            return {}

        manifest_path = diff_root / "manifest.json"
        manifest_data = {
            "base_backup": last_backup_time.isoformat(),
            "diff_chain": [],
            "last_diff_timestamp": None
        }

        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest_data = json.load(f)
            except Exception as e:
                self._logger.warning(f"Failed to read existing manifest, creating new: {e}")

        exported_files = {}

        for schema, table_name in tables:
            self._messenger.info(f"Last backup (UTC): {last_backup_time}")
            self._messenger.info(f"Last row in DB: {self.get_max_updated_at(table_name, schema, basis)}")
            if not self._schema_exporter._column_exists(schema, table_name, basis):
                self._messenger.warning(f"Skipping {table_name}: column '{basis}' does not exist")
                self._logger.warning(f"Table {table_name} skipped: no '{basis}' column")
                continue

            file_path = diff_dir / f"{table_name}_diff.csv"
            try:
                query = f'SELECT * FROM "{schema}"."{table_name}" WHERE {basis} > %s'
                connection = self._connection_provider.get_connection()
                with connection.cursor() as cur:
                    cur.execute(query, (last_backup_time,))
                    rows = cur.fetchall()
                    if not rows:
                        self._messenger.info(f"No new rows in {table_name} since last backup")
                        continue

                    columns = [d[0] for d in cur.description]
                    self._table_exporter._write_to_csv(file_path, columns, rows)
                    file_size = file_path.stat().st_size

                    exported_files[table_name] = {
                        "table_name": table_name,
                        "file_path": str(file_path),
                        "rows_count": len(rows),
                        "file_size": file_size
                    }
                    self._messenger.success(f"Diff {table_name}: {len(rows)} rows → {file_path.name}")
                    self._logger.info(f"Diff export {table_name}: {len(rows)} rows, {file_size/1024:.2f} KB")

            except Exception as e:
                self._messenger.error(f"Diff export failed for {table_name}: {e}")
                self._logger.error(f"Diff export failed for {table_name}: {e}")

        if exported_files:
            manifest_data["diff_chain"].append(diff_timestamp)
            manifest_data["last_diff_timestamp"] = diff_timestamp
            try:
                with open(manifest_path, "w", encoding="utf-8") as f:
                    json.dump(manifest_data, f, indent=4, ensure_ascii=False)
                oschmod.set_mode(manifest_path, "600")
                self._messenger.info(f"Manifest updated: {manifest_path}")
            except Exception as e:
                self._messenger.error(f"Failed to update manifest: {e}")
                self._logger.error(f"Manifest update failed: {e}")

        return exported_files

    def _prepare_output_directory(self, outpath: Path) -> bool:
        try:
            outpath.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            self._messenger.error(f"Failed to create {outpath}: {e}")
            self._logger.error(f"Dir creation failed: {e}")
            return False

    def perform_differential_backup(self, basis: str, metadata_reader, tables: list = None):
        self._messenger.warning("Starting differential backup...")

        last_full_timestamp = metadata_reader.get_last_full_backup_timestamp()
        backup_location = metadata_reader.get_output_path_from_last_full_backup()

        if not last_full_timestamp or not backup_location:
            self._messenger.error("No previous full backup found. Cannot perform differential backup.")
            return False

        if not tables:
            tables = metadata_reader.get_table_names_from_last_full_backup()
            if not tables:
                self._messenger.error("No tables found in last full backup.")
                return False
            tables = [("public", t) for t in tables]

        self._messenger.info(f"Using basis column: {basis}")
        self._messenger.info(f"Tables: {[t[1] for t in tables]}")

        result = self.export_diff_table(
            tables=tables,
            last_backup_time=last_full_timestamp,
            outpath=backup_location,
            basis=basis
        )

        if result:
            self._messenger.success("Differential backup completed successfully.")
            return True
        else:
            self._messenger.error("Differential backup failed or no changes.")
            return False



class CompressionService:
    def __init__(self, logger: ILogger, messenger: IMessenger):
        self._logger = logger
        self._messenger = messenger

    def compress_backup(self, path):
        """Compress a backup directory to zip."""
        path = Path(path) if isinstance(path, str) else path
        if not path.exists() or not path.is_dir():
            self._messenger.error(f"Invalid path: {path}")
            return False
        try:
            zip_path = shutil.make_archive(str(path), 'zip', str(path))
            if zip_path:
                self._messenger.info("\n" + "="*60)
                self._messenger.success("Compressed backup location:")
                self._messenger.info(zip_path)
                self._messenger.info("="*60 + "\n")
                return True
            self._messenger.warning("Compression produced no file")
            return False
        except Exception as e:
            self._messenger.error(f"Compression failed: {e}")
            return False

class DifferentialBackupService:
    def __init__(self,
                 connection_provider: IConnectionProvider,
                 logger: ILogger,
                 messenger: IMessenger,
                 schema_exporter,
                 table_exporter):
        self._connection_provider = connection_provider
        self._logger = logger
        self._messenger = messenger
        self._schema_exporter = schema_exporter
        self._table_exporter = table_exporter

    def get_max_updated_at(self, table_name: str, schema: str, column: str):
        try:
            query = f'SELECT MAX({column}) FROM "{schema}"."{table_name}"'
            connection = self._connection_provider.get_connection()
            with connection.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()[0]
                return result.isoformat() if result else "None"
        except Exception as e:
            self._logger.error(f"get_max_updated_at failed: {e}")
            connection.rollback()
            return "Error"

    def export_diff_table(self, tables, last_backup_time: datetime, outpath: Path, basis: str) -> dict:
        self._messenger.warning(f"Exporting differential data since {last_backup_time} using '{basis}'")

        outpath = Path(outpath) if isinstance(outpath, str) else outpath
        diff_root = outpath / ".backup_diff"
        diff_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        diff_dir = diff_root / diff_timestamp

        if not self._prepare_output_directory(diff_dir):
            return {}

        manifest_path = diff_root / "manifest.json"
        manifest_data = {
            "base_backup": last_backup_time.isoformat(),
            "diff_chain": [],
            "last_diff_timestamp": None
        }

        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest_data = json.load(f)
            except Exception as e:
                self._logger.warning(f"Failed to read existing manifest, creating new: {e}")

        exported_files = {}

        for schema, table_name in tables:
            self._messenger.info(f"Last backup (UTC): {last_backup_time}")
            self._messenger.info(f"Last row in DB: {self.get_max_updated_at(table_name, schema, basis)}")
            if not self._schema_exporter._column_exists(schema, table_name, basis):
                self._messenger.warning(f"Skipping {table_name}: column '{basis}' does not exist")
                self._logger.warning(f"Table {table_name} skipped: no '{basis}' column")
                continue

            file_path = diff_dir / f"{table_name}_diff.csv"
            try:
                query = f'SELECT * FROM "{schema}"."{table_name}" WHERE {basis} > %s'
                connection = self._connection_provider.get_connection()
                with connection.cursor() as cur:
                    cur.execute(query, (last_backup_time,))
                    rows = cur.fetchall()
                    if not rows:
                        self._messenger.info(f"No new rows in {table_name} since last backup")
                        continue

                    columns = [d[0] for d in cur.description]
                    self._table_exporter._write_to_csv(file_path, columns, rows)
                    file_size = file_path.stat().st_size

                    exported_files[table_name] = {
                        "table_name": table_name,
                        "file_path": str(file_path),
                        "rows_count": len(rows),
                        "file_size": file_size
                    }
                    self._messenger.success(f"Diff {table_name}: {len(rows)} rows → {file_path.name}")
                    self._logger.info(f"Diff export {table_name}: {len(rows)} rows, {file_size/1024:.2f} KB")

            except Exception as e:
                self._messenger.error(f"Diff export failed for {table_name}: {e}")
                self._logger.error(f"Diff export failed for {table_name}: {e}")

        if exported_files:
            manifest_data["diff_chain"].append(diff_timestamp)
            manifest_data["last_diff_timestamp"] = diff_timestamp
            try:
                with open(manifest_path, "w", encoding="utf-8") as f:
                    json.dump(manifest_data, f, indent=4, ensure_ascii=False)
                oschmod.set_mode(manifest_path, "600")
                self._messenger.info(f"Manifest updated: {manifest_path}")
            except Exception as e:
                self._messenger.error(f"Failed to update manifest: {e}")
                self._logger.error(f"Manifest update failed: {e}")

        return exported_files

    def _prepare_output_directory(self, outpath: Path) -> bool:
        try:
            outpath.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            self._messenger.error(f"Failed to create {outpath}: {e}")
            self._logger.error(f"Dir creation failed: {e}")
            return False

    def perform_differential_backup(self, basis: str, metadata_reader, tables: list = None):
        self._messenger.warning("Starting differential backup...")

        last_full_timestamp = metadata_reader.get_last_full_backup_timestamp()
        backup_location = metadata_reader.get_output_path_from_last_full_backup()

        if not last_full_timestamp or not backup_location:
            self._messenger.error("No previous full backup found. Cannot perform differential backup.")
            return False

        if not tables:
            tables = metadata_reader.get_table_names_from_last_full_backup()
            if not tables:
                self._messenger.error("No tables found in last full backup.")
                return False
            tables = [("public", t) for t in tables]

        self._messenger.info(f"Using basis column: {basis}")
        self._messenger.info(f"Tables: {[t[1] for t in tables]}")

        result = self.export_diff_table(
            tables=tables,
            last_backup_time=last_full_timestamp,
            outpath=backup_location,
            basis=basis
        )

        if result:
            self._messenger.success("Differential backup completed successfully.")
            return True
        else:
            self._messenger.error("Differential backup failed or no changes.")
            return False
