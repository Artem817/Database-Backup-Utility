import csv
from pathlib import Path
import psycopg2
from psycopg2.extensions import connection

from mixins.backup_catalog_mixin import BackupCatalogMixin
from mixins.conection_config_mixin import ConnectionConfigMixin
from factory import DatabaseClient
from mixins.facade_mixin import ServiceFacadeMixin
from mixins.orchestration_mixin import BackupOrchestrationMixin
from mixins.differential_mixin import DifferentialBackupMixin
from services.backup.exporters import SchemaExporter
from services.interfaces import IConnectionProvider

class PostgresClient(ConnectionConfigMixin,
                     BackupCatalogMixin,
                     BackupOrchestrationMixin,
                     DifferentialBackupMixin,
                     ServiceFacadeMixin,
                     DatabaseClient,
                     IConnectionProvider):

    def __init__(self, host, database, user, password, **kwargs):
        if 'port' not in kwargs:
            kwargs['port'] = 5432
        super().__init__(host, database, user, password, **kwargs)
        self._connection: connection = None

    # unique
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

    # unique
    def disconnect(self):
        try:
            if self._connection and not self._connection.closed:
                self._connection.close()
                self._connection = None
                self._messenger.info("Disconnected from database.")
                self._logger.info("Database connection closed")

            return None
        except psycopg2.OperationalError as e:
            self._messenger.error(f"Error on disconnect: {e}")
            self._logger.error(f"Disconnect failed: {e}")
            return None

    @property
    def connection(self):
        return self._connection

    @property
    def is_connected(self):
        return self._connection is not None and not self._connection.closed

    def get_connection(self):
        return self._connection

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

    def database_schema(self, output_path: Path) -> str | None:
        """Export database schema using pg_dump."""
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.export_schema(output_path)

    def _export_single_table(self, schema: str, table_name: str, file_path: Path, metadata=None) -> dict | None:
        """Export a single table to CSV."""
        try:
            full_table_name = f'"{schema}"."{table_name}"'
            
            with self._connection.cursor() as cur:
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

    def get_database_size(self) -> str:
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.get_database_size()

    def execute_query(self, query: str):
        from services.execution.executor import QueryExecutor
        query_executor = QueryExecutor(self, self._logger, self._messenger)
        return query_executor.execute_query(query)
