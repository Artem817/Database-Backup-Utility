import csv
from pathlib import Path
import psycopg2
from psycopg2.extensions import connection
from datetime import datetime, timezone
import json
import oschmod

from factory import DatabaseClient
from custom_logging import BackupLogger, BackupCatalog
from console_utils import get_messenger
from services.backup.core import DifferentialBackupService, CompressionService
from services.backup.exporters import SchemaExporter, TableExporter
from services.backup.metadata import BackupMetadataReader, BackupHistoryService
from services.execution.executor import QueryExecutor
from services.execution.exporter import QueryResultExporter
from services.interfaces import IConnectionProvider


class PostgresClient(DatabaseClient, IConnectionProvider):
    def __init__(self, host, database, user, password, logger: BackupLogger = None, messenger=None, port=5432, utility_version="1.0.0"):
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port
        self._connection: connection = None
        self._utility_version = utility_version
        self._database_version = None
        self.compress: bool = False
        self._logger = logger if logger is not None else BackupLogger(name=f"backup_{database}", log_file=f"backup_{database}.log")
        self._messenger = messenger if messenger is not None else get_messenger()

    @property
    def connection(self):
        return self._connection

    @property
    def database_name(self):
        return self._database

    @property
    def connection_params(self):
        return {"host": self._host, "user": self._user, "database": self._database, "port": self._port, "password": self._password}

    def get_connection(self):
        return self._connection
    
    def get_connection_params(self):
        return self.connection_params

    @property
    def is_connected(self):
        return self._connection is not None and not self._connection.closed

    def connect(self):
        try:
            self._connection = psycopg2.connect(
                dbname=self._database, user=self._user, host=self._host,
                password=self._password, port=self._port, connect_timeout=10
            )
            with self._connection.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                self._database_version = version.split(',')[0]
                self._messenger.success("PostgreSQL connection successful!")
                self._messenger.info(f"  Server version: {self._database_version}")
                self._logger.info(f"Connected to database: {self._database} ({self._database_version})")
            return self._connection
        except psycopg2.OperationalError as e:
            self._messenger.error(f"Unable to connect. Details: {e}")
            self._messenger.warning("Check your .env/CLI settings.")
            self._logger.error(f"Connection failed: {e}")
            return None

    def disconnect(self):
        try:
            if self._connection and not self._connection.closed:
                self._connection.close()
                self._connection = None
                self._messenger.info("Disconnected from database.")
                self._logger.info("Database connection closed")
        except psycopg2.OperationalError as e:
            self._messenger.error(f"Error on disconnect: {e}")
            self._logger.error(f"Disconnect failed: {e}")
            return None

    def _execute(self, query):
        cur = self._connection.cursor()
        cur.execute(query)
        return cur

    def fetch_all(self, query):
        cur = self._execute(query)
        return cur.fetchall()

    def fetch_one(self, query):
        cur = self._execute(query)
        return cur.fetchone()

    def commit(self):
        return self._connection.commit()

    def rollback(self):
        return self._connection.rollback()

    def validate_connection(self):
        try:
            if not self._connection or self._connection.closed:
                return False
            with self._connection.cursor() as cur:
                cur.execute("SELECT 1;")
                return cur.fetchone()[0] == 1
        except Exception:
            return False

    # Legacy methods that still need to be present for compatibility
    def get_tables(self):
        """Return list of (schema, table_name) for user tables."""
        table_exporter = TableExporter(self, self._logger, self._messenger)
        return table_exporter.get_tables()
    
    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.table_exists(table_name, schema)
    
    def get_backup_history(self, limit: int = 10) -> list:
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        return metadata_reader.get_backup_history(limit)
    
    def get_last_full_backup_timestamp(self) -> str | None:
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        return metadata_reader.get_last_full_backup_timestamp()
    
    def get_table_names_from_last_full_backup(self) -> list[str]:
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        return metadata_reader.get_table_names_from_last_full_backup()
    
    def get_output_path_from_last_full_backup(self) -> str | None:
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        return metadata_reader.get_output_path_from_last_full_backup()
    
    def database_schema(self, output_path: Path) -> str | None:
        """Export database schema using pg_dump."""
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.export_schema(output_path)
    
    def _export_single_table(self, schema: str, table_name: str, file_path: Path, metadata=None) -> dict | None:
        """Export a single table to CSV."""
        try:
            full_table_name = f'"{schema}"."{table_name}"'
            
            with self.connection.cursor() as cur:
                cur.execute(f"SELECT * FROM {full_table_name}")
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
                
                self._write_table_to_csv(file_path, columns, rows)
                file_size = file_path.stat().st_size
                
                self._messenger.success(
                    f"Exported {table_name}: {len(rows)} rows, {file_size/1024:.2f} KB"
                )
                
                if metadata:
                    self._log_table_backup(metadata, table_name, len(rows), file_size, str(file_path))
                
                return {
                    "table_name": table_name,
                    "file_path": str(file_path),
                    "rows_count": len(rows),
                    "file_size": file_size
                }
                
        except Exception as e:
            self._messenger.error(f"Export {table_name} failed: {e}")
            self._logger.error(f"Table export failed for {table_name}: {e}")
            return None

    # Methods that use new service classes
    def print_backup_history(self, limit: int = 10):
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        history_service = BackupHistoryService(metadata_reader, self._messenger)
        history_service.print_backup_history(limit)
    
    def csv_fragmental_backup(self, rows, outpath, query: str = None):
        query_result_exporter = QueryResultExporter(self._logger, self._messenger, self._database)
        return query_result_exporter.export_csv(rows, outpath, query)
    
    def extract_sql_query(self, query: str, outpath):
        query_executor = QueryExecutor(self, self._logger, self._messenger)
        query_result_exporter = QueryResultExporter(self._logger, self._messenger, self._database)
        return query_executor.extract_sql_query(query, outpath, query_result_exporter)
    
    def compress_backup(self, path):
        compression_service = CompressionService(self._logger, self._messenger)
        return compression_service.compress_backup(path)
    
    def perform_differential_backup(self, basis: str, tables: list = None):
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        table_exporter = TableExporter(self, self._logger, self._messenger)
        diff_service = DifferentialBackupService(self, self._logger, self._messenger, schema_exporter, table_exporter)
        return diff_service.perform_differential_backup(basis, metadata_reader, tables)
    
    def export_diff_table(self, tables, last_backup_time: datetime, outpath: Path, basis: str) -> dict:
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        table_exporter = TableExporter(self, self._logger, self._messenger)
        diff_service = DifferentialBackupService(self, self._logger, self._messenger, schema_exporter, table_exporter)
        return diff_service.export_diff_table(tables, last_backup_time, outpath, basis)
    
    def get_max_updated_at(self, table_name: str, schema: str, column: str):
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        table_exporter = TableExporter(self, self._logger, self._messenger)
        diff_service = DifferentialBackupService(self, self._logger, self._messenger, schema_exporter, table_exporter)
        return diff_service.get_max_updated_at(table_name, schema, column)
    
    def get_last_backup_path(self) -> str | None:
        catalog = BackupCatalog()
        return catalog.get_last_backup().get("backup_location") if catalog.get_last_backup() else None

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
            backup_structure = self._create_backup_structure(base_path, metadata["id"], back_up_time=metadata["timestamp_start"])
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

    def _column_exists(self, schema: str, table_name: str, column: str) -> bool:
        try:
            with self.connection.cursor() as cur:
                cur.execute("SAVEPOINT check_column")
                try:
                    cur.execute("""
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s AND column_name = %s;
                    """, (schema, table_name, column))
                    result = cur.fetchone() is not None
                    cur.execute("RELEASE SAVEPOINT check_column")
                    return result
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT check_column")
                    return False
        except Exception as e:
            self._logger.error(f"Column check failed: {e}")
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

    def execute_query(self, query: str):
        query_executor = QueryExecutor(self, self._logger, self._messenger)
        return query_executor.execute_query(query)

    def get_database_size(self) -> str:
        """Delegate to SchemaExporter to get formatted database size."""
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.get_database_size()

    def get_table_schema(self, table_name: str, schema: str = "public"):
        """Delegate to SchemaExporter to fetch table schema info."""
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.get_table_schema(table_name, schema)
