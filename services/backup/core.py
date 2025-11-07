import json
import shutil
from datetime import datetime
from pathlib import Path
import subprocess
import os

import oschmod

from services.backup.metadata import BackupMetadataReader
from services.interfaces import IConnectionProvider, ILogger, IMessenger

class DifferentialBackupService:
    def __init__(self,
                 connection_provider: IConnectionProvider,
                 logger: ILogger,
                 messenger: IMessenger):
        self._connection_provider = connection_provider
        self._logger = logger
        self._messenger = messenger

    def perform_differential_backup(self, metadata_reader: BackupMetadataReader) -> bool:
        """Creates a differential backup using WAL archiving (PostgreSQL) or xtrabackup (MySQL)"""
        connection_params = self._connection_provider.get_connection_params()
        
        if connection_params.get("port") == 3306:
            # MySQL incremental backup using xtrabackup
            return self._perform_mysql_differential_backup(metadata_reader)
        else:
            # PostgreSQL differential backup using WAL archiving
            return self._perform_postgres_differential_backup(metadata_reader)
    
    def _perform_postgres_differential_backup(self, metadata_reader: BackupMetadataReader) -> bool:
        """Creates a differential PostgreSQL backup by archiving WAL files since last full backup"""
        self._messenger.warning("Starting differential WAL backup...")
        
        connection_params = self._connection_provider.get_connection_params()
        
        metadata = self._logger.start_backup(
            backup_type="differential",
            database=connection_params["database"],
            database_version="WAL-based",
            utility_version="pg_wal_archiving",
            compress=True 
        )
        
        last_full_backup_location = metadata_reader.get_output_path_from_last_full_backup()
        last_full_timestamp = metadata_reader.get_last_full_backup_timestamp()
        
        if not last_full_backup_location or not last_full_timestamp:
            self._messenger.error("No previous full backup found. Cannot perform differential backup.")
            self._logger.finish_backup(metadata, success=False)
            return False
        
        full_backup_path = Path(last_full_backup_location)
        differentials_dir = full_backup_path / "differentials"
        
        if not differentials_dir.exists():
            differentials_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        diff_backup_dir = differentials_dir / f"diff_{timestamp}_{metadata['id'].split('_')[-1]}"
        diff_backup_dir.mkdir(parents=True, exist_ok=True)
        
        wal_archive_dir = diff_backup_dir / "wal_archive"
        wal_archive_dir.mkdir(parents=True, exist_ok=True)
        
        try:
            connection = self._connection_provider.get_connection()
            with connection.cursor() as cur:
                cur.execute("SELECT pg_current_wal_lsn();")
                current_lsn = cur.fetchone()[0]
                
                cur.execute("SELECT pg_walfile_name(pg_current_wal_lsn());")
                current_wal_file = cur.fetchone()[0]
                
                cur.execute("SHOW data_directory;")
                data_dir = cur.fetchone()[0]
                
                full_backup_wal = full_backup_path / "pg_wal.tar.gz"
                
                if not full_backup_wal.exists():
                    self._messenger.error(f"Full backup WAL archive not found: {full_backup_wal}")
                    self._logger.finish_backup(metadata, success=False)
                    return False
                
                import tarfile
                import tempfile
                
                last_backup_wal_file = None
                with tempfile.TemporaryDirectory() as tmpdir:
                    with tarfile.open(full_backup_wal, 'r:gz') as tar:
                        wal_members = [m for m in tar.getmembers() if m.isfile() and not m.name.endswith('.history')]
                        if wal_members:
                            wal_members.sort(key=lambda x: x.name)
                            last_backup_wal_file = wal_members[-1].name.split('/')[-1]
                
                if not last_backup_wal_file:
                    self._messenger.warning("Could not determine last WAL file from full backup")
                    last_backup_wal_file = "000000010000000000000001"
                
                self._messenger.info(f"Last full backup WAL file: {last_backup_wal_file}")
                self._messenger.info(f"Current WAL LSN: {current_lsn}")
                self._messenger.info(f"Current WAL file: {current_wal_file}")
                
                if last_backup_wal_file >= current_wal_file:
                    self._messenger.warning("No new WAL files since last backup (database unchanged)")
                    self._logger.info("No changes detected - no new WAL files")
                    
                    metadata["backup_location"] = str(diff_backup_dir)
                    metadata["backup_size_bytes"] = 0
                    metadata["wal_files_count"] = 0
                    metadata["parent_backup_location"] = str(full_backup_path)
                    metadata["current_lsn"] = current_lsn
                    
                    self._logger.finish_backup(metadata, success=True)
                    return True
                
                cur.execute("SELECT pg_switch_wal();")
                switch_lsn = cur.fetchone()[0]
                self._messenger.info(f"Switched WAL to LSN: {switch_lsn}")
            
            pg_wal_dir = Path(data_dir) / "pg_wal"
            
            if not pg_wal_dir.exists():
                self._messenger.error(f"WAL directory not found: {pg_wal_dir}")
                self._logger.error(f"pg_wal directory not accessible: {pg_wal_dir}")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            wal_files_to_archive = []
            
            for wal_file in sorted(pg_wal_dir.glob("0*")):
                if wal_file.is_file() and wal_file.name > last_backup_wal_file:
                    if '.' not in wal_file.name or wal_file.suffix == '.partial':
                        if wal_file.suffix != '.partial':
                            wal_files_to_archive.append(wal_file)
            
            if not wal_files_to_archive:
                self._messenger.warning("No new WAL files to archive")
                
                metadata["backup_location"] = str(diff_backup_dir)
                metadata["backup_size_bytes"] = 0
                metadata["wal_files_count"] = 0
                metadata["parent_backup_location"] = str(full_backup_path)
                metadata["current_lsn"] = current_lsn
                
                # Save metadata
                metadata_file = diff_backup_dir / "metadata.json"
                with open(metadata_file, 'w') as f:
                    json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
                
                self._logger.finish_backup(metadata, success=True)
                return True
            
            self._messenger.info(f"Archiving {len(wal_files_to_archive)} WAL files...")
            
            compressed_count = 0
            
            for wal_file in wal_files_to_archive:
                dest_file = wal_archive_dir / wal_file.name
                
                try:
                    shutil.copy2(wal_file, dest_file)
                    
                    gzip_cmd = ["gzip", str(dest_file)]
                    subprocess.run(gzip_cmd, check=True, capture_output=True)
                    compressed_count += 1
                    
                except Exception as e:
                    self._logger.warning(f"Failed to archive {wal_file.name}: {e}")
            
            # Calculate total size
            total_size = sum(f.stat().st_size for f in diff_backup_dir.rglob('*') if f.is_file())
            
            self._messenger.success(f"Differential backup created at {diff_backup_dir}")
            self._messenger.info(f"WAL files archived: {compressed_count}")
            self._messenger.info(f"Backup size: {total_size / (1024**2):.2f} MB")
            
            metadata["backup_location"] = str(diff_backup_dir)
            metadata["backup_size_bytes"] = total_size
            metadata["wal_files_count"] = compressed_count
            metadata["current_lsn"] = current_lsn
            metadata["current_wal_file"] = current_wal_file
            metadata["parent_backup_location"] = str(full_backup_path)
            metadata["last_backup_wal_file"] = last_backup_wal_file
            
            # Save metadata.json
            metadata_file = diff_backup_dir / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            self._messenger.info(f"Metadata saved: {metadata_file}")
            
            # Update chain.json
            chain_file = differentials_dir / "chain.json"
            if chain_file.exists():
                with open(chain_file, 'r') as f:
                    chain_data = json.load(f)
            else:
                chain_data = {
                    "full_backup_id": full_backup_path.name,
                    "full_backup_time": last_full_timestamp.isoformat() if hasattr(last_full_timestamp, 'isoformat') else str(last_full_timestamp),
                    "differentials": []
                }
            
            chain_data["differentials"].append({
                "id": diff_backup_dir.name,
                "timestamp": metadata["timestamp_start"],
                "wal_files_count": compressed_count,
                "size_bytes": total_size,
                "current_lsn": current_lsn
            })
            
            with open(chain_file, 'w') as f:
                json.dump(chain_data, f, indent=2, ensure_ascii=False)
            self._messenger.info(f"Chain updated: {chain_file}")
            
            self._logger.finish_backup(metadata, success=True)
            return True
        
        except PermissionError as e:
            self._messenger.error(f"Permission denied accessing WAL directory: {e}")
            self._messenger.warning("Try running with user that has access to PostgreSQL data directory")
            self._logger.error(f"Permission denied: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False
        except Exception as e:
            self._messenger.error(f"Differential backup failed: {e}")
            self._logger.error(f"Differential backup failed: {e}")
            import traceback
            self._logger.error(traceback.format_exc())
            self._logger.finish_backup(metadata, success=False)
            return False
    
    def _perform_mysql_differential_backup(self, metadata_reader: BackupMetadataReader) -> bool:
        """Creates a differential MySQL backup using xtrabackup"""
        self._messenger.warning("Starting MySQL differential backup with xtrabackup...")
        
        metadata = self._logger.start_backup(
            backup_type="differential",
            database=self._connection_provider.get_connection_params()["database"],
            database_version="Physical",
            utility_version="xtrabackup",
            compress=True
        )
        
        timestamp_start = datetime.fromisoformat(metadata["timestamp_start"].replace('Z', '+00:00'))
        
        last_full_backup_location = metadata_reader.get_output_path_from_last_full_backup()
        
        if not last_full_backup_location:
            self._messenger.error("No previous full backup found. Cannot perform incremental backup.")
            self._logger.finish_backup(metadata, success=False)
            return False
        
        base_path = Path(last_full_backup_location).parent
        backup_id = metadata["id"]
        backup_dir = base_path / backup_id
        backup_dir.mkdir(parents=True, exist_ok=True)
        
        connection_params = self._connection_provider.get_connection_params()
        
        login_path = connection_params.get('login_path')
        socket = connection_params.get('socket')
        
        if login_path:
            xtrabackup_cmd = [
                "xtrabackup",
                "--backup",
                f"--target-dir={backup_dir}",
                f"--incremental-basedir={last_full_backup_location}",
                f"--login-path={login_path}",
                "--compress",
                "--compress-threads=4"
            ]
            
            if socket:
                xtrabackup_cmd.append(f"--socket={socket}")
            
            env = os.environ.copy()
            self._messenger.info(f"Using login-path '{login_path}' for xtrabackup authentication")
        else:
            xtrabackup_cmd = [
                "xtrabackup",
                "--backup",
                f"--target-dir={backup_dir}",
                f"--incremental-basedir={last_full_backup_location}",
                f"--user={connection_params['user']}",
                f"--host={connection_params['host']}",
                f"--port={connection_params['port']}",
                "--compress",
                "--compress-threads=4"
            ]
            
            env = os.environ.copy()
            env['MYSQL_PWD'] = connection_params['password']
        
        try:
            self._messenger.info(f"Running xtrabackup incremental from: {last_full_backup_location}")
            
            process = subprocess.run(
                xtrabackup_cmd,
                capture_output=True,
                check=False,
                text=True,
                env=env
            )
            
            if process.returncode != 0:
                error_msg = process.stderr or "Unknown error"
                self._messenger.error(f"xtrabackup incremental failed: {error_msg}")
                self._logger.error(f"xtrabackup incremental failed: {error_msg}")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            checkpoints_file = backup_dir / "xtrabackup_checkpoints"
            if not checkpoints_file.exists():
                self._messenger.error("xtrabackup_checkpoints not found - backup may be incomplete")
                self._logger.error("xtrabackup_checkpoints file not found")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            total_size = sum(f.stat().st_size for f in backup_dir.rglob('*') if f.is_file())
            
            self._messenger.success(f"MySQL incremental backup created at {backup_dir}")
            self._messenger.info(f"Backup size: {total_size / (1024**2):.2f} MB")
            
            metadata["backup_location"] = str(backup_dir)
            metadata["backup_size_bytes"] = total_size
            metadata["backup_checkpoints_path"] = str(checkpoints_file)
            metadata["parent_backup_location"] = last_full_backup_location
            
            self._logger.finish_backup(metadata, success=True)
            return True
        
        except Exception as e:
            self._messenger.error(f"MySQL incremental backup failed: {e}")
            self._logger.error(f"MySQL incremental backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False