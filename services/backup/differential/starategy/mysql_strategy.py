from services.backup.differential.strategy_base import IDifferentialBackupStrategy
from services.backup.metadata import BackupMetadataReader
import json
import subprocess
from datetime import datetime
from pathlib import Path


class MySQLDifferentialBackupStrategy(IDifferentialBackupStrategy):
    def __init__(self, connection_provider, logger, messenger):
        self._connection_provider = connection_provider
        self._logger = logger
        self._messenger = messenger
    
    def write_metadata_file(self, metadata: dict, output_path: Path) -> bool:
        """Writes the backup metadata to a JSON file"""
        try:
            metadata_file = output_path / "metadata.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            self._messenger.info(f"Metadata saved: {metadata_file}")
            return True
        except Exception as e:
            self._messenger.error(f"Failed to write metadata file: {e}")
            self._logger.error(f"Failed to write metadata file: {e}")
            return False
    
    def perform_differential_backup(self, metadata_reader: BackupMetadataReader) -> bool:
        """Creates a differential MySQL backup using xtrabackup --incremental"""
        self._messenger.warning("Starting MySQL differential backup with xtrabackup...")
        
        connection_params = self._connection_provider.get_connection_params()
        
        metadata = self._logger.start_backup(
            backup_type="differential",
            database=connection_params["database"],
            database_version="xtrabackup-based",
            utility_version="xtrabackup",
            compress=True
        )
        
        last_full_backup_location = metadata_reader.get_output_path_from_last_full_backup()
        
        if not last_full_backup_location:
            self._messenger.error("No previous full backup found. Cannot perform differential backup.")
            self._logger.finish_backup(metadata, success=False)
            return False
        
        full_backup_path = Path(last_full_backup_location)
        
        if not full_backup_path.exists():
            self._messenger.error(f"Full backup directory not found: {full_backup_path}")
            self._logger.finish_backup(metadata, success=False)
            return False
        
        # Store differential backups NEXT TO full backup
        backup_root_dir = full_backup_path.parent
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        diff_backup_dir = backup_root_dir / f"differential_{connection_params['database']}_{timestamp}_{metadata['id'].split('_')[-1]}"
        diff_backup_dir.mkdir(parents=True, exist_ok=True)
        
        # Save reference to base full backup
        base_backup_ref = diff_backup_dir / "base_backup_id.txt"
        base_backup_ref.write_text(full_backup_path.name)
        
        try:
            # Build xtrabackup incremental command
            user = str(connection_params.get('user', '')).strip('"').strip("'")
            password = connection_params.get('password', '')
            
            xtrabackup_cmd = [
                "xtrabackup",
                "--backup",
                f"--target-dir={diff_backup_dir}",
                f"--incremental-basedir={full_backup_path}",
                f"--host={connection_params.get('host', 'localhost')}",
                f"--port={connection_params.get('port', 3306)}",
                f"--user={user}",
                "--compress",
                "--compress-threads=4"
            ]
            
            # Add password if provided
            if password:
                xtrabackup_cmd.append(f"--password={password}")
            
            self._messenger.info(f"Running xtrabackup incremental backup...")
            self._logger.info(f"Command: {' '.join([c if '--password' not in c else '--password=***' for c in xtrabackup_cmd])}")
            
            result = subprocess.run(
                xtrabackup_cmd,
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                self._messenger.error(f"xtrabackup failed: {result.stderr}")
                self._logger.error(f"xtrabackup stderr: {result.stderr}")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            # Calculate backup size
            total_size = sum(f.stat().st_size for f in diff_backup_dir.rglob('*') if f.is_file())
            
            self._messenger.success(f"Differential backup created at {diff_backup_dir}")
            self._messenger.info(f"Backup size: {total_size / (1024**2):.2f} MB")
            
            metadata["backup_location"] = str(diff_backup_dir)
            metadata["backup_size_bytes"] = total_size
            metadata["parent_backup_location"] = str(full_backup_path)
            metadata["parent_backup_id"] = full_backup_path.name
            metadata["xtrabackup_output"] = result.stdout[-500:] if result.stdout else ""
            
            # Save metadata
            self.write_metadata_file(metadata, diff_backup_dir)
            
            self._logger.finish_backup(metadata, success=True)
            return True
            
        except FileNotFoundError:
            self._messenger.error("xtrabackup utility not found. Please install Percona XtraBackup.")
            self._logger.error("xtrabackup not installed")
            self._logger.finish_backup(metadata, success=False)
            return False
        except Exception as e:
            self._messenger.error(f"Differential backup failed: {e}")
            self._logger.error(f"Differential backup error: {e}")
            import traceback
            self._logger.error(traceback.format_exc())
            self._logger.finish_backup(metadata, success=False)
            return False
