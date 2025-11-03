import os
from pathlib import Path
import psycopg2
from psycopg2.extensions import connection
from datetime import datetime

from mixins.backup_catalog_mixin import BackupCatalogMixin
from mixins.conection_config_mixin import ConnectionConfigMixin
from factory import DatabaseClient
from mixins.facade_mixin import ServiceFacadeMixin
from mixins.orchestration_mixin import BackupOrchestrationMixin
from mixins.differential_mixin import DifferentialBackupMixin
from services.backup.exporters import SchemaExporter
from services.interfaces import IConnectionProvider
from typing import Optional, Tuple, Any, List
from decorators.types_decorators import not_none
import subprocess
from decorators.replication_privilege import requires_replication_privilege
from decorators.check_basebackup_decorator import check_basebackup

class PostgresClient(ConnectionConfigMixin,
                     BackupCatalogMixin,
                     BackupOrchestrationMixin,
                     DifferentialBackupMixin,
                     ServiceFacadeMixin,
                     DatabaseClient,
                     IConnectionProvider):

    def __init__(self, host: str, database: str, user: str, password: str, **kwargs: Any) -> None:
        if 'port' not in kwargs:
            kwargs['port'] = 5432
        super().__init__(host, database, user, password, **kwargs)
        self._connection: Optional[connection] = None

    # unique
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

    # unique
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

    @not_none('output_path')
    def database_schema(self, output_path: Path) -> str | None:
        """Export database schema using pg_dump."""
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.export_schema(output_path)

    def get_database_size(self) -> str:
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.get_database_size()

    @not_none('query')
    def execute_query(self, query: str) -> Any:
        from services.execution.executor import QueryExecutor
        query_executor = QueryExecutor(self, self._logger, self._messenger)
        return query_executor.execute_query(query)

    def create_full_backup_zstd(self):
        """Create full backup using zstd, unique function for PostgresClient"""
        pass   
    
    @check_basebackup
    @requires_replication_privilege
    def backup_full(self, outpath: str) -> bool:
        """Creates a full PostgreSQL backup using pg_basebackup"""
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        self._messenger.info(f"Starting full backup → {base_path}")
        
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
            back_up_time=metadata["timestamp_start"]
        )
        
        # pg_basebackup creates directory structure itself
        backup_dir = backup_structure["backup_root"] / "base"
        
        pg_basebackup_cmd = [
            "pg_basebackup",
            "-h", self._host,
            "-p", str(self._port),
            "-U", self._user,
            "-D", str(backup_dir),     # Directory for backup
            "-F", "t",                 # Tar format
            "-X", "stream",            # Stream WAL during backup (better than fetch)
            "--checkpoint=fast"        # Force immediate checkpoint
        ]              
        
        # Add compression if specified
        if hasattr(self, '_compressing_level') and self._compressing_level:
            pg_basebackup_cmd.extend(["-z", "-Z", str(self._compressing_level)])
        
        # Set progress reporting
        pg_basebackup_cmd.append("-P")
                        
        env = os.environ.copy()
        env['PGPASSWORD'] = self._password
        
        try:
            self._messenger.info("Running pg_basebackup... (this may take a while)")
            
            pg_basebackup_process = subprocess.run(
                pg_basebackup_cmd,
                capture_output=True,
                env=env,
                check=False,
                text=True
            )
            
            if pg_basebackup_process.returncode != 0:
                error_msg = pg_basebackup_process.stderr or "Unknown error"
                self._messenger.error(f"pg_basebackup failed: {error_msg}")
                self._logger.error(f"pg_basebackup failed: {error_msg}")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            # Check if backup was created
            if not backup_dir.exists():
                self._messenger.error("Backup directory was not created")
                self._logger.error("Backup directory was not created")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            # Calculate backup size
            total_size = sum(f.stat().st_size for f in backup_dir.rglob('*') if f.is_file())
            
            self._messenger.success(f"Full backup created at {backup_dir}")
            self._messenger.info(f"Backup size: {total_size / (1024**2):.2f} MB")
            self._logger.info(f"Full backup directory: {backup_dir}")
            self._logger.info(f"Backup size: {total_size} bytes")
            
            metadata["backup_location"] = str(backup_structure["backup_root"])
            metadata["backup_size_bytes"] = total_size
            self._logger.finish_backup(metadata, success=True)
            return True
        
        except Exception as e:
            self._messenger.error(f"Backup failed: {e}")
            self._logger.error(f"Backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False
    
    #FIXME pg_dump 
    def partial_backup(self, tables: List[str], outpath: str, backup_type: str = "partial") -> bool:
        """Creates a partial PostgreSQL backup for specified tables, compressing it with Zstd"""
        
        if not tables:
            self._messenger.error("No tables specified for partial backup")
            return False
            
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        tables_str = ", ".join(tables)
        self._messenger.info(f"Starting partial backup of tables [{tables_str}] → {base_path}")
        
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
            back_up_time=metadata["timestamp_start"]  # Pass ISO string
        )
        
        backup_filename = f"{self._database}_partial_{timestamp_start.strftime('%Y%m%d_%H%M%S')}.sql.zst"
        backup_file_path = backup_structure["backup_root"] / backup_filename
        
        pg_dump_cmd = [
            "pg_dump",
            "-h", self._host,
            "-p", str(self._port),
            "-U", self._user,
            "-Fc"
        ]
        
        for table in tables:
            pg_dump_cmd.extend(["-t", table])
            
        pg_dump_cmd.append(self._database)
        
        zstd_cmd = [
            "zstd",
            "-o", str(backup_file_path),
            "-"
        ]
        
        env = os.environ.copy()
        env['PGPASSWORD'] = self._password
        
        try:
            pg_dump_process = subprocess.Popen(
                pg_dump_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )

            zstd_process = subprocess.Popen(
                zstd_cmd,
                stdin=pg_dump_process.stdout,
                stderr=subprocess.PIPE
            )
            
            pg_dump_process.stdout.close()
            
            pg_dump_process.wait()
            zstd_process.wait()
            
            if pg_dump_process.returncode != 0:
                _, pg_dump_stderr = pg_dump_process.communicate()
                self._messenger.error(f"pg_dump failed: {pg_dump_stderr.decode()}")
                self._logger.error(f"pg_dump failed: {pg_dump_stderr.decode()}")
                self._logger.finish_backup(metadata, success=False)
                return False
                
            if zstd_process.returncode != 0:
                _, zstd_stderr = zstd_process.communicate()
                self._messenger.error(f"Backup failed during compression: {zstd_stderr.decode()}")
                self._logger.error(f"Backup failed during compression: {zstd_stderr.decode()}")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            self._messenger.success(f"Partial backup created at {backup_file_path}")
            self._messenger.info(f"Backed up tables: {tables_str}")
            self._logger.info(f"Partial backup file: {backup_file_path}")
            self._logger.info(f"Tables included: {tables_str}")
            metadata["backup_location"] = str(backup_structure["backup_root"])  # Save directory, not file path
            metadata["tables"] = tables
            self._logger.finish_backup(metadata, success=True)
            return True
        
        except Exception as e:
            self._messenger.error(f"Partial backup failed: {e}")
            self._logger.error(f"Partial backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False




