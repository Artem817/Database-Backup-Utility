import os
from pathlib import Path
import psycopg2
from psycopg2.extensions import connection
from datetime import datetime, timezone

from mixins.backup_catalog_mixin import BackupCatalogMixin
from mixins.conection_config_mixin import ConnectionConfigMixin
from mixins.differential_mixin import DifferentialBackupMixin
from factory import DatabaseClient
from services.interfaces import IConnectionProvider
from typing import Optional, Tuple, Any, List
from decorators.types_decorators import not_none
import subprocess
from decorators.replication_privilege import _check_archive_mode, requires_replication_privilege, _check_wal_level
from decorators.check_basebackup_decorator import check_basebackup
from services.backup.archive_utils import create_single_archive
from cli.postgres_wal_config import PostgresWalArchiveConfig

class PostgresClient(ConnectionConfigMixin,
                     BackupCatalogMixin,
                     DifferentialBackupMixin,
                     DatabaseClient,
                     IConnectionProvider):

    def __init__(self, host: str, database: str, user: str, password: str, **kwargs: Any) -> None:
        if 'port' not in kwargs:
            kwargs['port'] = 5432
        self._database_engine = "postgresql"
        
        self._use_pgpass = kwargs.pop('use_pgpass', False)
        
        super().__init__(host, database, user, password, **kwargs)
        self._connection: Optional[connection] = None
        
        self._archive_path: Optional[str] = None
        self._wal_config = PostgresWalArchiveConfig()
        self._configure_wal_archive()

    def _configure_wal_archive(self) -> None:
        """Configure WAL archive directory on initialization"""
        try:
            self._archive_path = self._wal_config.configure_archive_directory()
            
            if self._archive_path:
                self._messenger.info(f"WAL archive directory: {self._archive_path}")
                self._logger.info(f"WAL archive path configured: {self._archive_path}")
            else:
                self._messenger.warning("WAL archive directory not configured")
                self._logger.warning("WAL archive path not configured")
        except Exception as e:
            self._messenger.error(f"Failed to configure WAL archive: {e}")
            self._logger.error(f"WAL archive configuration error: {e}")
            self._archive_path = None

    @property
    def archive_path(self) -> Optional[str]:
        """Get the configured WAL archive path"""
        return self._archive_path

    def connect(self) -> Optional[connection]:
        try:
            connect_kwargs = {
                "dbname": self._database,
                "user": self._user,
                "host": self._host,
                "port": self._port,
                "connect_timeout": 10,
            }

            # When .pgpass is enabled, do not pass an explicit password so libpq
            # can resolve credentials from ~/.pgpass.
            if not self._use_pgpass and self._password:
                connect_kwargs["password"] = self._password

            self._connection = psycopg2.connect(**connect_kwargs)
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

    @not_none('query')
    def extract_sql_query(self, query: str, outpath):
        from services.execution.executor import QueryExecutor
        from services.execution.exporter import QueryResultExporter

        query_executor = QueryExecutor(self, self._logger, self._messenger)
        query_result_exporter = QueryResultExporter(
            self._logger, self._messenger, self._database
        )
        return query_executor.extract_sql_query(
            query, outpath, query_result_exporter
        )
    
    @_check_archive_mode
    @_check_wal_level
    @check_basebackup
    @requires_replication_privilege
    def backup_full(
        self,
        outpath: str,
        single_archive: bool = True,
        storage='local',
        validate_restore: bool = True,
        restore_validation_port: int = 0,
        restore_validation_keep_data: bool = False,
        restore_validation_checksums: bool = True,
        pg_bin_dir: str | None = None,
    ) -> bool:
        """Create full database backup with zstd compression"""
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        self._messenger.info(f"Starting full backup → {base_path}")
        
        metadata = self._logger.start_backup(
            backup_type="full",
            database=self._database,
            database_type=self._database_engine,
            database_version=self._database_version or "Unknown",
            utility_version="pg_basebackup",
            compress=True,
            storage=storage
        )
        
        timestamp_start = datetime.fromisoformat(metadata["timestamp_start"].replace('Z', '+00:00'))
        backup_id = metadata["id"]
        
        backup_dir = base_path / backup_id
        backup_dir.mkdir(parents=True, exist_ok=True)
        metadata["backup_location"] = str(backup_dir)
        
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
            "-v",
            '-l', f"UTIL_FULL_{backup_id}", 
            '--manifest-checksums=SHA256' 
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
            metadata["base_tar_path"] = str(base_tar)
            metadata["wal_tar_path"] = str(wal_tar) if wal_tar.exists() else ""
            
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

            if self._connection and not self._connection.closed:
                with self._connection.cursor() as cur:
                    cur.execute("SELECT pg_current_wal_lsn();")
                    metadata["current_lsn"] = cur.fetchone()[0]
                    cur.execute("SELECT pg_walfile_name(pg_current_wal_lsn());")
                    metadata["current_wal_file"] = cur.fetchone()[0]
            
            total_size = sum(f.stat().st_size for f in backup_dir.rglob('*') if f.is_file())
            
            self._messenger.success(f"Full backup created at {backup_dir}")
            self._messenger.info(f"Files: base.tar.gz, pg_wal.tar.gz")
            self._messenger.info(f"Backup size: {total_size / (1024**2):.2f} MB")
            self._logger.info(f"Full backup directory: {backup_dir}")
            self._logger.info(f"Backup size: {total_size} bytes")
            
            metadata["backup_size_bytes"] = total_size
            metadata["wal_archived"] = True
            metadata["backup_format"] = "tar+gzip"
            metadata["restore_validation_required"] = bool(validate_restore)
            
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

            validation_passed = True
            if validate_restore:
                validation_passed = self._run_restore_validation(
                    metadata=metadata,
                    backup_dir=backup_dir,
                    restore_validation_port=restore_validation_port,
                    restore_validation_keep_data=restore_validation_keep_data,
                    restore_validation_checksums=restore_validation_checksums,
                    pg_bin_dir=pg_bin_dir,
                )
            
            self._logger.finish_backup(metadata, success=validation_passed)
            return validation_passed
        
        except Exception as e:
            self._messenger.error(f"Backup failed: {e}")
            self._logger.error(f"Backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False

    def _run_restore_validation(
        self,
        metadata: dict,
        backup_dir: Path,
        restore_validation_port: int,
        restore_validation_keep_data: bool,
        restore_validation_checksums: bool,
        pg_bin_dir: str | None,
    ) -> bool:
        from services.validation.postgres_restore_validation import (
            PostgresRestoreValidationService,
        )

        self._messenger.section_header("Automated Restore Validation")

        if not self._archive_path:
            error = "WAL archive directory is not configured"
            metadata["restore_validation"] = {
                "schema_version": "1.0",
                "mode": "postgresql_test_restore",
                "state": "failed",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "error": error,
            }
            self._messenger.error(error)
            self._logger.error(error)
            return False

        validation_backup_path = Path(metadata.get("archive_path") or backup_dir)

        validator = PostgresRestoreValidationService(
            backup_path=validation_backup_path,
            archive_dir=self._archive_path,
            source_connection_params=self.connection_params,
            restore_port=restore_validation_port,
            verify_database=self._database,
            verify_user=self._user or "postgres",
            pg_bin_dir=pg_bin_dir,
            checksum_tables=restore_validation_checksums,
            keep_data_dir=restore_validation_keep_data,
            logger=self._logger,
            messenger=self._messenger,
        )
        report = validator.validate()
        metadata["restore_validation"] = report
        return report.get("state") == "passed"
