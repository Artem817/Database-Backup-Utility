from clients.postgres_client import PostgresClient

class BackupService:
    def __init__(self, dbclient: PostgresClient) -> None:
        self.dbclient = dbclient
    
    def full_backup(self, parsed_args) -> None:
        """Handle full database backup with parsed arguments"""
        if not parsed_args.path:
            raise ValueError("Path is required. Use: full database -path <path>")
        
        self.dbclient.backup_full(outpath=parsed_args.path)
    
    def partial_backup(self, parsed_args) -> None:
        """Handle partial table backup with parsed arguments"""
        if not parsed_args.path:
            raise ValueError("Path is required. Use: full tables -path <path>")
        if not parsed_args.tablename:
            raise ValueError("Provide at least one -tablename <name>")
        
        self.dbclient.partial_backup(
            tables=parsed_args.tablename, 
            outpath=parsed_args.path
        )
    
    def differential_backup(self, parsed_args) -> None:
        """Handle differential backup with parsed arguments"""
        check_last_full = self.dbclient.get_tables()
        
        if not check_last_full:
            raise ValueError("No full backup found. Differential backup cannot proceed.")
        
        basis = "updated_at"  
        self.dbclient.perform_differential_backup(basis=basis, tables=check_last_full)
    
    def execute_sql(self, sql_query, parsed_args) -> None:
        """Handle SQL execution with parsed arguments"""
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
    
    def _print_sql_preview(self, rows: list, limit: int = 10):
        """Helper method to print SQL results preview"""
        if not rows:
            print("No rows returned")
            return
        for i, row in enumerate(rows):
            if i < limit:
                print(row)
            elif i == limit:
                print(f"... {len(rows) - limit} more rows hidden")
                break



