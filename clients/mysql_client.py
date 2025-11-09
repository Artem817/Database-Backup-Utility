import os
from pathlib import Path
from datetime import datetime
import shutil
from decorators.utility_available import check_utility_available
from factory import DatabaseClient
from mixins.backup_catalog_mixin import BackupCatalogMixin
from mixins.conection_config_mixin import ConnectionConfigMixin
from mixins.differential_mixin import DifferentialBackupMixin
from services.interfaces import IConnectionProvider
import pymysql
import pymysql.cursors
from pymysql import err
from pymysql.connections import Connection
from typing import Optional, List
import subprocess
from services.backup.archive_utils import create_single_archive


class MysqlClient(ConnectionConfigMixin,
                  BackupCatalogMixin,
                  DifferentialBackupMixin,
                  DatabaseClient,
                  IConnectionProvider):

    def __init__(self, host, database, user, password, **kwargs):
        if 'port' not in kwargs:
            kwargs['port'] = 3306
        
        login_path = kwargs.pop('login_path', None)
        socket = kwargs.pop('socket', None)
        
        super().__init__(host, database, user, password, **kwargs)
        self._connection: Optional[Connection] = None
        
        self._login_path = login_path
        self._socket = socket
        
        if self._login_path:
            self._extract_login_path_config()
    
    def _extract_login_path_config(self):
        """Extract connection details from MySQL login-path"""
        try:
            result = subprocess.run(
                ["mysql_config_editor", "print", f"--login-path={self._login_path}"],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                for line in result.stdout.split('\n'):
                    if '=' in line:
                        key, value = line.split('=', 1)
                        key = key.strip()
                        value = value.strip()
                        
                        if key == 'host' and not self._host:
                            self._host = value
                        elif key == 'user' and not self._user:
                            self._user = value
                        elif key == 'port' and not self._port:
                            self._port = int(value)
                            
                self._logger.info(f"Extracted config from login-path '{self._login_path}'")
        except Exception as e:
            self._logger.warning(f"Could not extract login-path config: {e}")

    @property
    def connection(self):
        return self._connection

    @property
    def is_connected(self) -> bool:
        """Check if database connection is active"""
        if self._login_path:
            return self._connection == "login_path_mode"
        
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
            self._logger.info("Connecting to database...")
            self._messenger.info("Connecting to database...")
            
            if self._login_path:
                self._messenger.info(f"Using MySQL login-path for authentication: {self._login_path}")
                self._logger.info(f"Skipping pymysql connection - will use login-path with xtrabackup")
                
                self._connection = "login_path_mode"
                self._messenger.success(f"Login-path configured: {self._login_path}")
                self._logger.info(f"MySQL client configured with login-path: {self._login_path}")
                return self._connection  
            
            self._messenger.info("Attempting MySQL connection...")
            
            self._connection = pymysql.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                database=self._database,
                cursorclass=pymysql.cursors.DictCursor
            )
            
            with self._connection.cursor() as cursor:
                cursor.execute("SELECT VERSION() as version")
                result = cursor.fetchone()
                self._database_version = result["version"]
            
            self._messenger.success(f"Connected to MySQL {self._database_version}")
            self._messenger.info(f"Database: {self._database}")
            self._logger.info(f"Successfully connected to MySQL {self._database_version}")
            
            return self._connection
            
        except err.OperationalError as e:
            error_message = f"Unable to connect. Details: {e}"
            self._messenger.error(error_message)
            self._messenger.warning("Check your .env/CLI settings.")
            self._logger.error(f"Connection failed: {e}")
            raise

    def disconnect(self):
        try:
            if self._login_path and self._connection == "login_path_mode":
                self._connection = None
                self._messenger.info("Login-path session ended.")
                self._logger.info("Login-path mode disconnected")
                return
            
            if self._connection and self.is_connected:
                self._connection.close()
                self._connection = None
                self._messenger.info("Disconnected from database.")
                self._logger.info("Database connection closed")
        except err.OperationalError as e:
            self._messenger.error(f"Error on disconnect: {e}")
            self._logger.error(f"Disconnect failed: {e}")
        except Exception as e:
            self._messenger.error(f"✗ Error closing connection: {e}")
            self._logger.error(f"Error closing connection: {e}")

    def validate_connection(self):
        try:
            if not self.is_connected:
                return False
            
            if self._login_path:
                result = subprocess.run(
                    ["mysql", f"--login-path={self._login_path}", "-e", "SELECT 1;", self._database],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                return result.returncode == 0
            
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
        
    @check_utility_available("xtrabackup")
    def backup_full(self, outpath: str, single_archive: bool = True) -> bool:
        """Create full database backup with zstd compression"""
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
        
        if self._login_path:
            xtrabackup_cmd = [
                "xtrabackup",
                "--backup",
                f"--target-dir={backup_dir}",
                f"--login-path={self._login_path}",
                "--compress",
                "--compress-threads=4"
            ]
            
            if self._socket:
                xtrabackup_cmd.append(f"--socket={self._socket}")
            
            env = os.environ.copy()
            self._messenger.info(f"Using login-path '{self._login_path}' for xtrabackup authentication")
        else:
            xtrabackup_cmd = [
                "xtrabackup",
                "--backup",
                f"--target-dir={backup_dir}",
                f"--user={self._user}",
                f"--host={self._host}",
                f"--port={self._port}",
                "--compress",
                "--compress-threads=4"
            ]
            
            env = os.environ.copy()
            env['MYSQL_PWD'] = self._password
        
        try:
            self._messenger.info("Running xtrabackup... (this may take a while)")
            
            process = subprocess.run(
                xtrabackup_cmd,
                capture_output=True,
                check=False,
                text=True,
                env=env
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
            
            binlog_info_file = backup_dir / "xtrabackup_binlog_info"
            if binlog_info_file.exists():
                with open(binlog_info_file, 'r') as f:
                    binlog_info = f.read().strip()
                    metadata["binlog_info"] = binlog_info
                    self._logger.info(f"Binlog info: {binlog_info}")
            
            if single_archive:
                self._messenger.section_header("Creating Single Archive (zstd)")
                archive_path = create_single_archive(backup_dir, self._logger, self._messenger)
                if archive_path:
                    metadata["archive_path"] = str(archive_path)
                    metadata["archive_format"] = "tar+zstd"
                    metadata["archive_size_bytes"] = archive_path.stat().st_size
                    self._messenger.success(f"✓ Single archive ready: {archive_path.name}")
                else:
                    self._messenger.warning("Single archive creation skipped/failed - backup remains as directory")
            
            self._logger.finish_backup(metadata, success=True)
            return True
        
        except Exception as e:
            self._messenger.error(f"MySQL backup failed: {e}")
            self._logger.error(f"MySQL backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False