import os
from pathlib import Path
import psycopg2
from psycopg2.extensions import connection
from datetime import datetime

from mixins.backup_catalog_mixin import BackupCatalogMixin
from mixins.conection_config_mixin import ConnectionConfigMixin
from mixins.differential_mixin import DifferentialBackupMixin
from factory import DatabaseClient
from services.interfaces import IConnectionProvider
from typing import Optional, Tuple, Any, List
from decorators.types_decorators import not_none
import subprocess
from decorators.replication_privilege import requires_replication_privilege, _check_wal_level
from decorators.check_basebackup_decorator import check_basebackup
import json
from services.backup.archive_utils import create_single_archive

class PostgresClient(ConnectionConfigMixin,
                     BackupCatalogMixin,
                     DifferentialBackupMixin,
                     DatabaseClient,
                     IConnectionProvider):

    def __init__(self, host: str, database: str, user: str, password: str, **kwargs: Any) -> None:
        if 'port' not in kwargs:
            kwargs['port'] = 5432
        
        self._use_pgpass = kwargs.pop('use_pgpass', False)
        
        super().__init__(host, database, user, password, **kwargs)
        self._connection: Optional[connection] = None

    def connect(self) -> Optional[connection]:
        try:
            self._connection = psycopg2.connect(
                dbname=self._database, user=self._user, host=self._host,
                password=self._password, port=self._port, connect_timeout=10
            )
            with self._connection.cursor() as cur:
                cur.execute("SELECT version();")
                
                version_tuple: Optional[Tuple[Any, ...]] = cur.fetchone()
                
                if version_tuple is None:
                    self._logger.error("Failed fetch version from database")
                    return None

                version: str = version_tuple[0]
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

    def disconnect(self) -> None:
        try:
            if self._connection and not self._connection.closed:
                self._connection.close()
                self._connection = None
                self._messenger.info("Disconnected from database.")
                self._logger.info("Database connection closed")
        except psycopg2.OperationalError as e:
            self._messenger.error(f"Error on disconnect: {e}")
            self._logger.error(f"Disconnect failed: {e}")

    @property
    def connection(self) -> Optional[connection]:
        return self._connection

    @property
    def is_connected(self) -> bool:
        return self._connection is not None and not self._connection.closed

    def get_connection(self) -> Optional[connection]:
        return self._connection

    @not_none('query')
    def _execute(self, query: str) -> Any:
        if self._connection is None:
            raise RuntimeError("No active database connection")
        cur = self._connection.cursor()
        cur.execute(query)
        return cur

    @not_none('query')
    def fetch_all(self, query: str) -> list[Any]:
        cur = self._execute(query)
        return cur.fetchall()

    @not_none('query')
    def fetch_one(self, query: str) -> Optional[Tuple[Any, ...]]:
        cur = self._execute(query)
        return cur.fetchone()

    def commit(self) -> None:
        if self._connection is None:
            raise RuntimeError("No active database connection")
        self._connection.commit()

    def rollback(self) -> None:
        if self._connection is None:
            raise RuntimeError("No active database connection")
        self._connection.rollback()

    def validate_connection(self) -> bool:
        try:
            if not self._connection or self._connection.closed:
                return False
            with self._connection.cursor() as cur:
                cur.execute("SELECT 1;")
                result = cur.fetchone()
                return result is not None and result[0] == 1
        except Exception:
            return False

    @not_none('query')
    def execute_query(self, query: str) -> Any:
        from services.execution.executor import QueryExecutor
        query_executor = QueryExecutor(self, self._logger, self._messenger)
        return query_executor.execute_query(query)
    
    @_check_wal_level
    @check_basebackup
    @requires_replication_privilege
    def backup_full(self, outpath: str, single_archive: bool = True) -> bool:
        """Create full database backup with zstd compression"""
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        self._messenger.info(f"Starting full backup → {base_path}")
        
        metadata = self._logger.start_backup(
            backup_type="full",
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version="pg_basebackup",
            compress=True 
        )
        
        timestamp_start = datetime.fromisoformat(metadata["timestamp_start"].replace('Z', '+00:00'))
        backup_id = metadata["id"]
        
        backup_dir = base_path / backup_id
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        pg_basebackup_cmd = [
            "pg_basebackup",
            "-h", self._host,
            "-p", str(self._port),
            "-U", self._user,
            "-D", str(backup_dir),
            "-F", "t",  # tar format 
            "-X", "stream",  
            "-c", "fast",  
            "-P", 
            "-v"  
        ]
        
        if hasattr(self, '_compressing_level') and self._compressing_level:
            pg_basebackup_cmd.extend(["-z", "-Z", str(self._compressing_level)])
        else:
            pg_basebackup_cmd.extend(["-z", "-Z", "6"])
        
        env = os.environ.copy()
        
        if self._use_pgpass:
            env.pop('PGPASSWORD', None)
            self._messenger.info("Using PostgreSQL .pgpass for authentication")
            metadata["auth_method"] = "pgpass"
        else:
            env['PGPASSWORD'] = self._password
            metadata["auth_method"] = "password"
        
        try:
            self._messenger.info("Running pg_basebackup... (this may take a while)")
            
            process = subprocess.run(
                pg_basebackup_cmd,
                capture_output=True,
                env=env,
                check=False,
                text=True
            )
            
            if process.returncode != 0:
                error_msg = process.stderr or "Unknown error"
                self._messenger.error(f"pg_basebackup failed: {error_msg}")
                self._logger.error(f"pg_basebackup failed: {error_msg}")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            base_tar = backup_dir / "base.tar.gz"
            wal_tar = backup_dir / "pg_wal.tar.gz"
            
            if not base_tar.exists():
                self._messenger.error("base.tar.gz not found - backup may be incomplete")
                self._logger.error("base.tar.gz file not found")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            # Check for backup_manifest (PostgreSQL 13+)
            manifest_path = backup_dir / "backup_manifest"
            if manifest_path.exists():
                metadata["backup_manifest_path"] = str(manifest_path)
                self._messenger.success(f"Backup manifest found at {manifest_path}")
                self._logger.info(f"Backup manifest path: {manifest_path}")
            else:
                self._messenger.warning("Backup manifest not found (PostgreSQL < 13)")
                metadata["backup_manifest_path"] = ""
            
            total_size = sum(f.stat().st_size for f in backup_dir.rglob('*') if f.is_file())
            
            self._messenger.success(f"Full backup created at {backup_dir}")
            self._messenger.info(f"Files: base.tar.gz, pg_wal.tar.gz")
            self._messenger.info(f"Backup size: {total_size / (1024**2):.2f} MB")
            self._logger.info(f"Full backup directory: {backup_dir}")
            self._logger.info(f"Backup size: {total_size} bytes")
            
            metadata["backup_location"] = str(backup_dir)
            metadata["backup_size_bytes"] = total_size
            metadata["wal_archived"] = True
            metadata["backup_format"] = "tar+gzip"
            
            metadata_file = backup_dir / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            self._messenger.info(f"Metadata saved: {metadata_file}")
            
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
            self._messenger.error(f"Backup failed: {e}")
            self._logger.error(f"Backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False




