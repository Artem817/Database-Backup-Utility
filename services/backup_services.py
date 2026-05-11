from custom_logging import BackupCatalog
from services.backup.metadata import BackupMetadataReader
from services.restore.mysql_restore import MysqlRestoreService
from services.restore.postgres_restore import PostgresRestoreService
from services.restore.pitr import (
    PITR_TIMEZONE_POLICY,
    PostgresPitrPlanner,
    format_recovery_target_time,
    parse_recovery_target_time,
)

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

    def restore_database(self, parsed_args) -> None:
        """Handle restore for the connected database engine."""
        engine = getattr(self.dbclient, "database_engine", None)
        if engine == "postgresql":
            return self.restore_postgres(parsed_args)
        if engine == "mysql":
            return self.restore_mysql(parsed_args)
        raise ValueError(f"Restore is not supported for database engine: {engine}")

    def restore_postgres(self, parsed_args) -> None:
        """Handle minimal PostgreSQL restore from the interactive console"""
        if getattr(self.dbclient, "database_engine", None) != "postgresql":
            raise ValueError("Restore command is currently implemented only for PostgreSQL")

        target_time = getattr(parsed_args, "target_time", None)
        backup_path = parsed_args.backup_path or parsed_args.path
        if not backup_path and not target_time:
            raise ValueError(
                "Backup path is required unless PITR target time is provided. Use: "
                "restore -path <full_backup_dir_or_archive> -data-dir <new_data_dir> "
                "[-wal-path <wal_archive_dir>] or restore -target-time "
                "\"2026-05-04 12:30:00\" -data-dir <new_data_dir>"
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

        recovery_target_time = None
        if target_time:
            if backup_path:
                recovery_target_time = format_recovery_target_time(
                    parse_recovery_target_time(str(target_time))
                )
                self.dbclient._messenger.info(f"Using explicit backup path for PITR: {backup_path}")
                self.dbclient._messenger.info(PITR_TIMEZONE_POLICY)
                self.dbclient._messenger.info(f"Recovery target time: {recovery_target_time}")
            else:
                pitr_plan = PostgresPitrPlanner(
                    database_name=self.dbclient.database_name,
                    target_time=str(target_time),
                    catalog_path=str(getattr(parsed_args, "catalog_path", "backup_catalog.db")),
                    messenger=self.dbclient._messenger,
                ).build_plan()
                backup_path = pitr_plan.backup_path
                recovery_target_time = pitr_plan.recovery_target_time

        restore_service = PostgresRestoreService(
            backup_path=str(backup_path),
            data_dir=str(parsed_args.data_dir),
            archive_dir=str(wal_archive_path),
            restore_port=parsed_args.restore_port or 55432,
            verify_database=self.dbclient.database_name,
            verify_user=parsed_args.verify_user or self.dbclient.connection_params.get("user") or "postgres",
            verify_query=parsed_args.verify_query,
            pg_bin_dir=str(parsed_args.pg_bin_dir) if parsed_args.pg_bin_dir else None,
            target_time=recovery_target_time,
            logger=self.dbclient._logger,
            messenger=self.dbclient._messenger,
        )

        if not restore_service.restore():
            raise RuntimeError("PostgreSQL restore failed")

    def restore_mysql(self, parsed_args) -> None:
        """Handle minimal MySQL restore from an XtraBackup directory."""
        if getattr(self.dbclient, "database_engine", None) != "mysql":
            raise ValueError("MySQL restore command requires a MySQL client session")

        if getattr(parsed_args, "target_time", None):
            raise ValueError("PITR target time restore is currently implemented only for PostgreSQL")

        backup_path = parsed_args.backup_path or parsed_args.path
        if not backup_path:
            raise ValueError(
                "Backup path is required. Use: restore -path <xtrabackup_dir> "
                "-data-dir <new_data_dir> -port <restore_port>"
            )

        if not parsed_args.data_dir:
            raise ValueError(
                "Data directory is required. Use: restore -path <xtrabackup_dir> "
                "-data-dir <new_data_dir> -port <restore_port>"
            )

        restore_port = (
            getattr(parsed_args, "port", None)
            or getattr(parsed_args, "restore_port", None)
            or 3308
        )

        restore_service = MysqlRestoreService(
            backup_path=str(backup_path),
            data_dir=str(parsed_args.data_dir),
            restore_port=int(restore_port),
            socket_path=str(
                getattr(parsed_args, "mysql_socket", None) or "/tmp/mysql_restore.sock"
            ),
            mysql_bin_dir=(
                str(parsed_args.mysql_bin_dir)
                if getattr(parsed_args, "mysql_bin_dir", None)
                else None
            ),
            owner_user=getattr(parsed_args, "mysql_owner", None),
            owner_group=getattr(parsed_args, "mysql_group", None),
            logger=self.dbclient._logger,
            messenger=self.dbclient._messenger,
        )

        if not restore_service.restore():
            raise RuntimeError("MySQL restore failed")
    
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
