from custom_logging import BackupCatalog
from services.backup.metadata import BackupMetadataReader
from services.restore.postgres_restore import PostgresRestoreService

class BackupService:
    def __init__(self, dbclient) -> None:
        self.dbclient = dbclient
    
    def full_backup(self, parsed_args) -> None:
        """Handle full physical backup"""
        if not parsed_args.path:
            raise ValueError("Path is required. Use: full database -path <path>")
        
        storage_type = getattr(parsed_args, 'storage_type', 'local')
        single_archive = getattr(parsed_args, 'single_archive', True)
        if getattr(self.dbclient, "database_engine", None) == "postgresql":
            self.dbclient.backup_full(
                outpath=parsed_args.path,
                single_archive=single_archive,
                storage=storage_type,
                validate_restore=getattr(parsed_args, "validate_restore", True),
                restore_validation_port=getattr(parsed_args, "restore_validation_port", 0),
                restore_validation_keep_data=getattr(
                    parsed_args,
                    "restore_validation_keep_data",
                    False,
                ),
                restore_validation_checksums=getattr(
                    parsed_args,
                    "restore_validation_checksums",
                    True,
                ),
                pg_bin_dir=(
                    str(parsed_args.pg_bin_dir)
                    if getattr(parsed_args, "pg_bin_dir", None)
                    else None
                ),
            )
            return

        self.dbclient.backup_full(
            outpath=parsed_args.path,
            single_archive=single_archive,
            storage=storage_type,
        )
        
        
    def differential_backup(self, parsed_args) -> None:
        """Handle differential backup for the current database"""
        metadata_reader = BackupMetadataReader(
            BackupCatalog(),
            self.dbclient._messenger,
            self.dbclient._logger,
            self.dbclient._database
        )
        
        self.dbclient.perform_differential_backup(metadata_reader)

    def restore_postgres(self, parsed_args) -> None:
        """Handle minimal PostgreSQL restore from the interactive console"""
        if getattr(self.dbclient, "database_engine", None) != "postgresql":
            raise ValueError("Restore command is currently implemented only for PostgreSQL")

        backup_path = parsed_args.backup_path or parsed_args.path
        if not backup_path:
            raise ValueError(
                "Backup path is required. Use: restore -path <full_backup_dir_or_archive> "
                "-data-dir <new_data_dir> [-wal-path <wal_archive_dir>]"
            )

        if not parsed_args.data_dir:
            raise ValueError(
                "Data directory is required. Use: restore -path <full_backup_dir_or_archive> "
                "-data-dir <new_data_dir> [-wal-path <wal_archive_dir>]"
            )

        wal_archive_path = (
            parsed_args.wal_path
            or parsed_args.archive_dir
            or getattr(self.dbclient, "archive_path", None)
        )
        if not wal_archive_path:
            raise ValueError(
                "WAL archive directory is required. Pass -wal-path <archive_dir> "
                "or configure archive_directory for PostgreSQL first."
            )

        restore_service = PostgresRestoreService(
            backup_path=str(backup_path),
            data_dir=str(parsed_args.data_dir),
            archive_dir=str(wal_archive_path),
            restore_port=parsed_args.restore_port,
            verify_database=self.dbclient.database_name,
            verify_user=parsed_args.verify_user or self.dbclient.connection_params.get("user") or "postgres",
            verify_query=parsed_args.verify_query,
            pg_bin_dir=str(parsed_args.pg_bin_dir) if parsed_args.pg_bin_dir else None,
            logger=self.dbclient._logger,
            messenger=self.dbclient._messenger,
        )

        if not restore_service.restore():
            raise RuntimeError("PostgreSQL restore failed")
    
    def execute_sql(self, sql_query, parsed_args) -> None:
        """Handle SQL execution"""
        if not sql_query:
            raise ValueError("No SQL query provided. Use: SQL <query>")
        
        if not parsed_args.extract:
            result = self.dbclient.execute_query(sql_query)
            if result is None:
                return
            rows, columns = result
            if columns:
                self._print_sql_preview(rows)
        else:
            if not parsed_args.path:
                raise ValueError("Path required. Use: SQL <query> -extract -path <path>")
            self.dbclient.extract_sql_query(sql_query, parsed_args.path)
