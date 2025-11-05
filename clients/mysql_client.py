import os
from pathlib import Path
from datetime import datetime
from factory import DatabaseClient
from mixins.backup_catalog_mixin import BackupCatalogMixin
from mixins.conection_config_mixin import ConnectionConfigMixin
from mixins.differential_mixin import DifferentialBackupMixin
from services.interfaces import IConnectionProvider
import pymysql.cursors
from pymysql import err
from pymysql.connections import Connection
from typing import Optional, List
import subprocess

class MysqlClient(ConnectionConfigMixin,
                  BackupCatalogMixin,
                  DifferentialBackupMixin,
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
        if self._connection is None:
            return False
        try:
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
                host=self._host,
                database=self._database,
                user=self._user,
                password=self._password,
                port=self._port,
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

    def disconnect(self):
        try:
            if self._connection and self.is_connected:
                self._connection.close()
                self._connection = None
                self._messenger.info("Disconnected from database.")
                self._logger.info("Database connection closed")
        except err.OperationalError as e:
            self._messenger.error(f"Error on disconnect: {e}")
            self._logger.error(f"Disconnect failed: {e}")

    def validate_connection(self):
        try:
            if not self.is_connected:
                return False
            with self._connection.cursor() as cur:
                cur.execute("SELECT 1 as test;")
                result = cur.fetchone()
                return result["test"] == 1
        except Exception:
            return False

    def execute_query(self, query: str):
        from services.execution.executor import QueryExecutor
        query_executor = QueryExecutor(self, self._logger, self._messenger)
        return query_executor.execute_query(query)

    def backup_full(self, outpath: str) -> bool:
        """Creates a full MySQL backup using xtrabackup (Percona XtraBackup)"""
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        self._messenger.info(f"Starting full MySQL backup with xtrabackup → {base_path}")
        
        metadata = self._logger.start_backup(
            backup_type="full",
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version="xtrabackup",
            compress=True
        )
        
        timestamp_start = datetime.fromisoformat(metadata["timestamp_start"].replace('Z', '+00:00'))
        backup_id = metadata["id"]
        
        backup_dir = base_path / backup_id
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        xtrabackup_cmd = [
            "xtrabackup",
            "--backup",
            f"--target-dir={backup_dir}",
            f"--user={self._user}",
            f"--password={self._password}",
            f"--host={self._host}",
            f"--port={self._port}",
            "--compress",
            "--compress-threads=4"
        ]
        
        try:
            self._messenger.info("Running xtrabackup... (this may take a while)")
            
            process = subprocess.run(
                xtrabackup_cmd,
                capture_output=True,
                check=False,
                text=True
            )
            
            if process.returncode != 0:
                error_msg = process.stderr or "Unknown error"
                self._messenger.error(f"xtrabackup failed: {error_msg}")
                self._logger.error(f"xtrabackup failed: {error_msg}")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            checkpoints_file = backup_dir / "xtrabackup_checkpoints"
            if not checkpoints_file.exists():
                self._messenger.error("xtrabackup_checkpoints not found - backup may be incomplete")
                self._logger.error("xtrabackup_checkpoints file not found")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            total_size = sum(f.stat().st_size for f in backup_dir.rglob('*') if f.is_file())
            
            self._messenger.success(f"Full MySQL backup created at {backup_dir}")
            self._messenger.info(f"Backup size: {total_size / (1024**2):.2f} MB")
            self._logger.info(f"Full backup directory: {backup_dir}")
            self._logger.info(f"Backup size: {total_size} bytes")
            
            metadata["backup_location"] = str(backup_dir)
            metadata["backup_size_bytes"] = total_size
            metadata["backup_checkpoints_path"] = str(checkpoints_file)
            
            self._logger.finish_backup(metadata, success=True)
            return True
        
        except Exception as e:
            self._messenger.error(f"MySQL backup failed: {e}")
            self._logger.error(f"MySQL backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False



