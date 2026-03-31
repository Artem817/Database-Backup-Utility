from services.backup.differential.strategy_base import DifferentialBackupStrategyBase
from services.backup.metadata import BackupMetadataReader
from datetime import datetime
import tarfile
import shutil
from pathlib import Path

from services.walvalidation.wal_check import WalChainValidation


class PostgresDifferentialBackupStrategy(DifferentialBackupStrategyBase):
    def __init__(self, connection_provider, logger, messenger):
        super().__init__(logger, messenger)
        self._connection_provider = connection_provider

    def perform_differential_backup(self, metadata_reader: BackupMetadataReader) -> bool:
        """
        Creates a differential PostgreSQL backup by copying WAL files
        from the user-configured archive_directory.
        """
        self._messenger.warning("Starting differential WAL backup...")

        connection_params = self._connection_provider.get_connection_params()

        if not hasattr(self._connection_provider, 'archive_path') or not self._connection_provider.archive_path:
            self._messenger.error("WAL archive directory not configured!")
            self._messenger.info("Please configure archive_directory in PostgreSQL settings.")
            return False

        archive_directory = Path(self._connection_provider.archive_path)

        if not archive_directory.exists():
            self._messenger.error(f"Archive directory does not exist: {archive_directory}")
            return False

        if not archive_directory.is_dir():
            self._messenger.error(f"Archive path is not a directory: {archive_directory}")
            return False

        metadata = self._logger.start_backup(
            backup_type="differential",
            database=connection_params["database"],
            database_type=connection_params.get("database_type", "postgresql"),
            database_version="WAL-based",
            utility_version="wal_archiving",
            compress=False,
            storage="local"
        )

        last_full_backup_location = metadata_reader.get_output_path_from_last_full_backup()

        if not last_full_backup_location:
            self._messenger.error("No previous full backup found. Cannot perform differential backup.")
            self._logger.finish_backup(metadata, success=False)
            return False

        full_backup_path = Path(last_full_backup_location)
        backup_root_dir = full_backup_path.parent

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        diff_backup_dir = backup_root_dir / f"differential_{connection_params['database']}_{timestamp}_{metadata['id'].split('_')[-1]}"
        diff_backup_dir.mkdir(parents=True, exist_ok=True)

        base_backup_ref = diff_backup_dir / "base_backup_id.txt"
        base_backup_ref.write_text(full_backup_path.name)

        try:
            connection = self._connection_provider.get_connection()
            if connection is None:
                self._messenger.error("No active PostgreSQL connection")
                self._logger.error("Connection provider returned None")
                self._logger.finish_backup(metadata, success=False)
                return False

            with connection.cursor() as cur:
                cur.execute("SELECT pg_current_wal_lsn();")
                current_lsn = cur.fetchone()[0]

                cur.execute("SELECT pg_walfile_name(pg_current_wal_lsn());")
                current_wal_file = cur.fetchone()[0]

                full_backup_wal = full_backup_path / "pg_wal.tar.gz"

                if not full_backup_wal.exists():
                    self._messenger.error(f"Full backup WAL archive not found: {full_backup_wal}")
                    self._logger.finish_backup(metadata, success=False)
                    return False

                last_backup_wal_file = None
                try:
                    with tarfile.open(full_backup_wal, 'r:gz') as tar:
                        wal_members = [
                            m for m in tar.getmembers()
                            if m.isfile() and not m.name.endswith('.history')
                        ]
                        if wal_members:
                            wal_members.sort(key=lambda x: x.name)
                            last_backup_wal_file = wal_members[-1].name.split('/')[-1]
                except tarfile.TarError as e:
                    self._messenger.error(f"Failed to read WAL archive from full backup: {e}")
                    self._logger.error(f"Failed to read pg_wal.tar.gz: {e}")
                    self._logger.finish_backup(metadata, success=False)
                    return False

                if not last_backup_wal_file:
                    self._messenger.warning("Could not determine last WAL file from full backup, using default")
                    last_backup_wal_file = "000000010000000000000001"

                self._messenger.info(f"Last full backup WAL file: {last_backup_wal_file}")
                self._messenger.info(f"Current WAL LSN: {current_lsn}")
                self._messenger.info(f"Current WAL file: {current_wal_file}")
                self._messenger.info(f"Archive directory: {archive_directory}")

                if last_backup_wal_file >= current_wal_file:
                    self._messenger.warning("No new WAL files since last backup (database unchanged)")
                    self._logger.info("No changes detected - no new WAL files")

                    base_metadata = self._build_common_metadata(
                        diff_backup_dir,
                        full_backup_path,
                        archive_directory,
                        current_lsn,
                        current_wal_file,
                        last_backup_wal_file,
                    )
                    base_metadata.update(
                        {
                            "backup_size_bytes": self._calculate_dir_size(diff_backup_dir),
                            "wal_files_count": 0,
                            "mode": "no_changes",
                        }
                    )

                    return self.finalize_backup(
                        metadata,
                        diff_backup_dir,
                        success=True,
                        extra_metadata=base_metadata,
                    )

                cur.execute("SELECT pg_switch_wal();")
                switch_lsn = cur.fetchone()[0]
                self._messenger.info(f"Switched WAL to LSN: {switch_lsn}")

                cur.execute("SELECT pg_walfile_name(pg_current_wal_lsn());")
                current_wal_file = cur.fetchone()[0]

            wal_files = sorted(
                f.name for f in archive_directory.glob("0*")
                if f.is_file() and '.' not in f.name  
            )

            new_wal_files = [name for name in wal_files if name > last_backup_wal_file]

            base_metadata = self._build_common_metadata(
                diff_backup_dir,
                full_backup_path,
                archive_directory,
                current_lsn,
                current_wal_file,
                last_backup_wal_file,
            )

            if not new_wal_files:
                self._messenger.warning("No new WAL files in archive since last full backup")
                self._logger.info("No WAL archived between backups")

                base_metadata.update(
                    {
                        "backup_size_bytes": self._calculate_dir_size(diff_backup_dir),
                        "wal_files_count": 0,
                        "mode": "no_new_wal",
                    }
                )

                return self.finalize_backup(
                    metadata,
                    diff_backup_dir,
                    success=True,
                    extra_metadata=base_metadata,
                )
            
            validator = WalChainValidation(
                archived_wal_files=new_wal_files,          
                last_full_backup_wal_file=last_backup_wal_file,
                current_wal_file=current_wal_file,
                wal_archive_directory=archive_directory,
                logger=self._logger,
                messenger=self._messenger,
            )

            if not validator.timeline_consistency_check():
                base_metadata.update({"mode": "wal_timeline_invalid"})
                return self.finalize_backup(
                    metadata, diff_backup_dir, success=False, extra_metadata=base_metadata
                )

            if not validator.validate_sequence_gaps():
                base_metadata.update({"mode": "wal_sequence_gap"})
                return self.finalize_backup(
                    metadata, diff_backup_dir, success=False, extra_metadata=base_metadata
                )

            if not validator.basic_wal_file_sanity_check():
                base_metadata.update({"mode": "wal_sanity_failed"})
                return self.finalize_backup(
                    metadata, diff_backup_dir, success=False, extra_metadata=base_metadata
                )

            first_wal = new_wal_files[0]
            last_wal = new_wal_files[-1]

            self._messenger.info(f"Found {len(new_wal_files)} new WAL files")
            self._messenger.info(f"WAL range: {first_wal} → {last_wal}")
            self._messenger.info("Copying WAL files to backup...")

            copied_count = 0
            for wal_name in new_wal_files:
                src = archive_directory / wal_name
                dst = diff_backup_dir / wal_name
                
                try:
                    shutil.copy2(src, dst)
                    copied_count += 1
                except Exception as e:
                    self._messenger.error(f"Failed to copy {wal_name}: {e}")
                    self._logger.error(f"Failed to copy WAL file {wal_name}: {e}")

            if copied_count == 0:
                self._messenger.error("Failed to copy any WAL files!")
                self._logger.error("No WAL files copied to backup")
                base_metadata.update({"mode": "wal_copy_failed", "wal_files_count": 0})
                return self.finalize_backup(
                    metadata, diff_backup_dir, success=False, extra_metadata=base_metadata
                )

            self._messenger.success(f"✓ Copied {copied_count}/{len(new_wal_files)} WAL files to backup")

            total_size = self._calculate_dir_size(diff_backup_dir)

            self._messenger.info(f"Differential backup size: {total_size / (1024**2):.2f} MB")

            base_metadata.update(
                {
                    "backup_size_bytes": total_size,
                    "wal_files_count": copied_count,
                    "wal_first_file": first_wal,
                    "wal_last_file": last_wal,
                    "mode": "wal_backup",
                }
            )

            self._messenger.success(f"✓ Differential backup completed: {diff_backup_dir}")
            return self.finalize_backup(
                metadata, diff_backup_dir, success=True, extra_metadata=base_metadata
            )

        except PermissionError as e:
            self._messenger.error(f"Permission denied accessing archive directory: {e}")
            self._messenger.warning("Check permissions on archive_directory")
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

    def _build_common_metadata(
        self,
        diff_backup_dir: Path,
        full_backup_path: Path,
        archive_directory: Path,
        current_lsn,
        current_wal_file,
        last_backup_wal_file,
    ) -> dict:
        """Compose metadata fields reused across success/failure branches."""
        return {
            "backup_location": str(diff_backup_dir),
            "parent_backup_location": str(full_backup_path),
            "parent_backup_id": full_backup_path.name,
            "current_lsn": current_lsn,
            "current_wal_file": current_wal_file,
            "last_backup_wal_file": last_backup_wal_file,
            "wal_archive_directory": str(archive_directory),
        }
