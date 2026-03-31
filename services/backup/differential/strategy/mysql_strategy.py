from services.backup.differential.strategy_base import DifferentialBackupStrategyBase
from services.backup.metadata import BackupMetadataReader
import os
import subprocess
from datetime import datetime
from pathlib import Path


class MySQLDifferentialBackupStrategy(DifferentialBackupStrategyBase):
    def __init__(self, connection_provider, logger, messenger):
        super().__init__(logger, messenger)
        self._connection_provider = connection_provider

    def perform_differential_backup(self, metadata_reader: BackupMetadataReader) -> bool:
        """Creates a differential MySQL backup using xtrabackup --incremental"""
        self._messenger.warning("Starting MySQL differential backup with xtrabackup...")
        
        connection_params = self._connection_provider.get_connection_params()
        
        metadata = self._logger.start_backup(
            backup_type="differential",
            database=connection_params["database"],
            database_type=connection_params.get("database_type", "mysql"),
            database_version="xtrabackup-based",
            utility_version="xtrabackup",
            compress=True,
            storage="local" 
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
        
        backup_root_dir = full_backup_path.parent
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        diff_backup_dir = backup_root_dir / f"differential_{connection_params['database']}_{timestamp}_{metadata['id'].split('_')[-1]}"
        diff_backup_dir.mkdir(parents=True, exist_ok=True)
        
        base_backup_ref = diff_backup_dir / "base_backup_id.txt"
        base_backup_ref.write_text(full_backup_path.name)
        
        try:
            user = str(connection_params.get('user', '')).strip('"').strip("'")
            password = connection_params.get('password', '')
            login_path = connection_params.get("login_path")
            socket = connection_params.get("socket")

            xtrabackup_cmd = [
                "xtrabackup",
                "--backup",
                f"--target-dir={diff_backup_dir}",
                f"--incremental-basedir={full_backup_path}",
                "--compress",
                "--compress-threads=4"
            ]

            env = None

            if login_path:
                xtrabackup_cmd.append(f"--login-path={login_path}")
                if socket:
                    xtrabackup_cmd.append(f"--socket={socket}")
            else:
                xtrabackup_cmd.extend(
                    [
                        f"--host={connection_params.get('host', 'localhost')}",
                        f"--port={connection_params.get('port', 3306)}",
                        f"--user={user}",
                    ]
                )
                if socket:
                    xtrabackup_cmd.append(f"--socket={socket}")
                if password:
                    env = os.environ.copy()
                    env["MYSQL_PWD"] = password
            
            self._messenger.info(f"Running xtrabackup incremental backup...")
            self._logger.info(f"Command: {' '.join(xtrabackup_cmd)}")
            
            result = subprocess.run(
                xtrabackup_cmd,
                capture_output=True,
                text=True,
                env=env
            )
            
            if result.returncode != 0:
                self._messenger.error(f"xtrabackup failed: {result.stderr}")
                self._logger.error(f"xtrabackup stderr: {result.stderr}")
                self._logger.finish_backup(metadata, success=False)
                return False

            checkpoints_file = diff_backup_dir / "xtrabackup_checkpoints"
            if not checkpoints_file.exists():
                self._messenger.error("xtrabackup_checkpoints not found - backup may be incomplete")
                self._logger.error("Incremental xtrabackup_checkpoints file not found")
                self._logger.finish_backup(metadata, success=False)
                return False
            
            total_size = sum(f.stat().st_size for f in diff_backup_dir.rglob('*') if f.is_file())
            
            self._messenger.success(f"Differential backup created at {diff_backup_dir}")
            self._messenger.info(f"Backup size: {total_size / (1024**2):.2f} MB")
            
            metadata["backup_location"] = str(diff_backup_dir)
            metadata["backup_size_bytes"] = total_size
            metadata["parent_backup_location"] = str(full_backup_path)
            metadata["parent_backup_id"] = full_backup_path.name
            metadata["backup_checkpoints_path"] = str(checkpoints_file)
            metadata["xtrabackup_output"] = result.stdout[-500:] if result.stdout else ""
            
            return self.finalize_backup(
                metadata,
                diff_backup_dir,
                success=True,
            )
            
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
