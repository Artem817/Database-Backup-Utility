import csv
from pathlib import Path
from factory import DatabaseClient
from mixins.backup_catalog_mixin import BackupCatalogMixin
from mixins.conection_config_mixin import ConnectionConfigMixin
from services.clients_mixin import fetch_version_database
from services.interfaces import IConnectionProvider
import pymysql.cursors
from pymysql import err
from pymysql.connections import Connection
from mixins.facade_mixin import ServiceFacadeMixin
from mixins.orchestration_mixin import BackupOrchestrationMixin
from mixins.differential_mixin import DifferentialBackupMixin

class MysqlClient(ConnectionConfigMixin,
                     BackupCatalogMixin,
                     BackupOrchestrationMixin,
                     DifferentialBackupMixin,
                     ServiceFacadeMixin,
                     DatabaseClient,
                     IConnectionProvider):

    def __init__(self, host, database, user, password, **kwargs):
        if 'port' not in kwargs:
            kwargs['port'] = 3306
        super().__init__(host, database, user, password, **kwargs)
        self._connection: Connection = None

    @property
    def connection(self):
        return self._connection

    @property
    def is_connected(self):
        """Check if MySQL connection is active"""
        if self._connection is None:
            return False
        try:
            # MySQL check the connection
            self._connection.ping(reconnect=False)
            return True
        except Exception:
            return False

    def get_connection(self):
        return self._connection

    def connect(self):
        try:
            self._messenger.info("Attempting MySQL connection...")
            self._connection = pymysql.connect(
                host= self._host,
                database= self._database,
                user= self._user,
                password= self._password,
                port= self._port,
                cursorclass=pymysql.cursors.DictCursor,
                connect_timeout=10  
            )
            
            with self._connection.cursor() as cur:
                cur.execute("SELECT VERSION() as version;")
                result = cur.fetchone()
                self._database_version = result['version'].split('-')[0]

            self._messenger.success("MySQL connection successful!")
            self._messenger.info(f"  Server version: {self._database_version}")
            self._logger.info(f"Connected to database: {self._database} ({self._database_version})")

            return self._connection

        except err.OperationalError as e:
            self._messenger.error(f"Unable to connect. Details: {e}")
            self._messenger.warning("Check your .env/CLI settings.")
            self._logger.error(f"Connection failed: {e}")
            return None
        except Exception as e:
            self._messenger.error(f"Unexpected connection error: {e}")
            self._logger.error(f"Connection failed: {e}")
            return None

    def disconnect(self):
        try:
            if self._connection and self.is_connected:
                self._connection.close()
                self._connection = None
                self._messenger.info("Disconnected from database.")
                self._logger.info("Database connection closed")

            return None
        except err.OperationalError as e:
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
            if not self.is_connected:
                return False
            with self._connection.cursor() as cur:
                cur.execute("SELECT 1 as test;")
                result = cur.fetchone()
                return result["test"] == 1  # MySQL returns dict cursor
        except Exception:
            return False

    def _export_single_table(self, schema: str, table_name: str, file_path: Path, metadata=None) -> dict | None:
        """Export a single table to CSV."""
        try:
            # MySQL doesn't use schema the same way as PostgreSQL
            full_table_name = f"`{table_name}`"
            
            with self._connection.cursor() as cur:
                cur.execute(f"SELECT * FROM {full_table_name}")
                rows = cur.fetchall()
                
                if rows:
                    columns = list(rows[0].keys())  # MySQL dict cursor
                    row_data = [[row[col] for col in columns] for row in rows]
                else:
                    columns = []
                    row_data = []
                
                self._write_table_to_csv(file_path, columns, row_data)
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

    def execute_query(self, query: str):
        from services.execution.executor import QueryExecutor
        query_executor = QueryExecutor(self, self._logger, self._messenger)
        return query_executor.execute_query(query)

    def get_database_size(self) -> str:
        from services.backup.exporters import SchemaExporter
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.get_database_size()

