import csv
from pathlib import Path
from datetime import datetime
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
from typing import Optional, List
import subprocess

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
        self._connection: Optional[Connection] = None

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

    def execute_query(self, query: str):
        from services.execution.executor import QueryExecutor
        query_executor = QueryExecutor(self, self._logger, self._messenger)
        return query_executor.execute_query(query)

    def get_database_size(self) -> str:
        from services.backup.exporters import SchemaExporter
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.get_database_size()

    def backup_full(self, outpath: str) -> bool:
        """Creates a full MySQL backup using mysqldump, compressing it with Zstd"""
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        self._messenger.info(f"Starting full MySQL backup → {base_path}")
        
        metadata = self._logger.start_backup(
            backup_type="full",
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version=self._utility_version,
            compress=True 
        )
        
        timestamp_start = datetime.fromisoformat(metadata["timestamp_start"].replace('Z', '+00:00'))
        
        backup_structure = self._create_backup_structure(
            base_path, 
            metadata["id"],
            back_up_time=metadata["timestamp_start"]  # Pass ISO string
        )
        
        backup_filename = f"{self._database}_{timestamp_start.strftime('%Y%m%d_%H%M%S')}.sql.zst"
        backup_file_path = backup_structure["backup_root"] / backup_filename
        
        # MySQL dump command
        mysqldump_cmd = [
            "mysqldump",
            "--host", self._host,
            "--port", str(self._port),
            "--user", self._user,
            f"--password={self._password}",
            "--single-transaction",
            "--routines",
            "--triggers",
            self._database
        ]
    
        zstd_cmd = [
            "zstd",
            "-o", str(backup_file_path),
            "-"
        ]
        
        try:
            mysqldump_process = subprocess.Popen(
                mysqldump_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            zstd_process = subprocess.Popen(
                zstd_cmd,
                stdin=mysqldump_process.stdout,
                stderr=subprocess.PIPE
            )
            
            mysqldump_process.stdout.close()
            
            mysqldump_process.wait()
            zstd_process.wait()
            
            if mysqldump_process.returncode != 0:
                _, mysqldump_stderr = mysqldump_process.communicate()
                self._messenger.error(f"mysqldump failed: {mysqldump_stderr.decode()}")
                self._logger.error(f"mysqldump failed: {mysqldump_stderr.decode()}")
                self._logger.finish_backup(metadata, success=False)
                return False
                
            if zstd_process.returncode != 0:
                _, zstd_stderr = zstd_process.communicate()
                self._messenger.error(f"Backup failed during compression: {zstd_stderr.decode()}")
                self._logger.error(f"Backup failed during compression: {zstd_stderr.decode()}")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            self._messenger.success(f"Full MySQL backup created at {backup_file_path}")
            self._logger.info(f"Full backup file: {backup_file_path}")
            metadata["backup_location"] = str(backup_structure["backup_root"]) 
            self._logger.finish_backup(metadata, success=True)
            return True
        
        except Exception as e:
            self._messenger.error(f"MySQL backup failed: {e}")
            self._logger.error(f"MySQL backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False

    def partial_backup(self, tables: List[str], outpath: str, backup_type: str = "partial") -> bool:
        """Creates a partial MySQL backup for specified tables, compressing it with Zstd"""
        
        if not tables:
            self._messenger.error("No tables specified for partial backup")
            return False
            
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        tables_str = ", ".join(tables)
        self._messenger.info(f"Starting partial MySQL backup of tables [{tables_str}] → {base_path}")
        
        metadata = self._logger.start_backup(
            backup_type=backup_type,
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version=self._utility_version,
            compress=True 
        )
        
        timestamp_start = datetime.fromisoformat(metadata["timestamp_start"].replace('Z', '+00:00'))
        
        backup_structure = self._create_backup_structure(
            base_path, 
            metadata["id"],
            back_up_time=metadata["timestamp_start"] 
        )
        
        backup_filename = f"{self._database}_partial_{timestamp_start.strftime('%Y%m%d_%H%M%S')}.sql.zst"
        backup_file_path = backup_structure["backup_root"] / backup_filename
        
        mysqldump_cmd = [
            "mysqldump",
            "--host", self._host,
            "--port", str(self._port),
            "--user", self._user,
            f"--password={self._password}",
            "--single-transaction",
            "--routines",
            "--triggers",
            self._database
        ]
        
        mysqldump_cmd.extend(tables)
        
        zstd_cmd = [
            "zstd",
            "-o", str(backup_file_path),
            "-"
        ]
        
        try:
            mysqldump_process = subprocess.Popen(
                mysqldump_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            zstd_process = subprocess.Popen(
                zstd_cmd,
                stdin=mysqldump_process.stdout,
                stderr=subprocess.PIPE
            )
            
            mysqldump_process.stdout.close()
            
            mysqldump_process.wait()
            zstd_process.wait()
            
            if mysqldump_process.returncode != 0:
                _, mysqldump_stderr = mysqldump_process.communicate()
                self._messenger.error(f"mysqldump failed: {mysqldump_stderr.decode()}")
                self._logger.error(f"mysqldump failed: {mysqldump_stderr.decode()}")
                self._logger.finish_backup(metadata, success=False)
                return False
                
            if zstd_process.returncode != 0:
                _, zstd_stderr = zstd_process.communicate()
                self._messenger.error(f"Backup failed during compression: {zstd_stderr.decode()}")
                self._logger.error(f"Backup failed during compression: {zstd_stderr.decode()}")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            self._messenger.success(f"Partial MySQL backup created at {backup_file_path}")
            self._messenger.info(f"Backed up tables: {tables_str}")
            self._logger.info(f"Partial backup file: {backup_file_path}")
            self._logger.info(f"Tables included: {tables_str}")
            metadata["backup_location"] = str(backup_structure["backup_root"])  # Save directory
            metadata["tables"] = tables
            self._logger.finish_backup(metadata, success=True)
            return True
        
        except Exception as e:
            self._messenger.error(f"Partial MySQL backup failed: {e}")
            self._logger.error(f"Partial MySQL backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False

