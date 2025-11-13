from clients.postgres_client import PostgresClient
from custom_logging import BackupCatalog
from services.backup.metadata import BackupMetadataReader

class BackupService:
    def __init__(self, dbclient: PostgresClient) -> None:
        self.dbclient = dbclient
    
    def full_backup(self, parsed_args) -> None:
        """Handle full WAL backup"""
        if not parsed_args.path:
            raise ValueError("Path is required. Use: full database -path <path>")
        
        storage_type = getattr(parsed_args, 'storage_type', 'local')
        single_archive = getattr(parsed_args, 'single_archive', True)
        self.dbclient.backup_full(outpath=parsed_args.path, single_archive=single_archive, storage=storage_type)
        
        
    def differential_backup(self, parsed_args) -> None:
        """Handle differential WAL backup"""
        metadata_reader = BackupMetadataReader(
            BackupCatalog(),
            self.dbclient._messenger,
            self.dbclient._logger,
            self.dbclient._database
        )
        
        self.dbclient.perform_differential_backup(metadata_reader)
    
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

