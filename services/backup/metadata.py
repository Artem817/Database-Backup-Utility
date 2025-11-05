from custom_logging import BackupCatalog
from services.interfaces import IMessenger, ILogger
from datetime import datetime, timezone

class BackupMetadataReader:
    def __init__(self, catalog: BackupCatalog, messenger: IMessenger, logger: ILogger, database: str):
        self._database = database
        self._catalog = catalog
        self._messenger = messenger
        self._logger = logger

    def _get_backups(self):
        return self._catalog.catalog.get("backups", [])
    
    def _get_last_full_backup_info(self, info_type: str) -> str | list[str] | None:
        self._messenger.info(f"Fetching last full backup info for type: {info_type}")
        backups = self._get_backups()
        
        self._messenger.info(f"Total backups found: {len(backups)}")
        full_backups = [
            backup for backup in backups
            if backup.get("database_name") == self._database and backup.get("type") == "full"
        ]
        self._messenger.info(f"Full backups for database '{self._database}': {len(full_backups)}")
        sorted_backups = sorted(full_backups, key=lambda b: b.get("timestamp_start", ""), reverse=True)
        if sorted_backups:
            last_backup = sorted_backups[0]
            self._messenger.info(f"Last full backup found: {last_backup['id']}")
            if info_type == "timestamp":
                ts = last_backup.get("timestamp_start")
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            elif info_type == "tables":
                tables = last_backup.get("tables", {})
                self._messenger.info(f"Tables in last full backup: {list(tables.keys())}")
                return list(tables.keys())
            elif info_type == "backup_location":
                return last_backup.get("backup_location")
            
        self._messenger.warning("No full backups found.")
        return None
    
    def get_last_full_backup_timestamp(self) -> str | None:
        return self._get_last_full_backup_info("timestamp")
    
    def last_full_manifest_path(self) -> str | None:
        return self._get_last_full_backup_info("backup_manifest_path")
    
    def get_table_names_from_last_full_backup(self) -> list[str]:
        return self._get_last_full_backup_info("tables") or []
 
    def get_output_path_from_last_full_backup(self) -> str | None:
        self._messenger.info("Retrieving last full backup location...")
        self._messenger.info(f"self._database: {self._database}")
        return self._get_last_full_backup_info("backup_location")
    
    def get_backup_diff_outpath(self) -> str | None:
        return self._get_last_full_backup_info("backup_diff_path")
    
    def get_backup_history(self, limit: int = 10) -> list:
        backups = self._get_backups()
        sorted_backups = sorted(backups, key=lambda b: b.get("timestamp_start", ""), reverse=True)
        return sorted_backups[:limit]

class BackupHistoryService:
    def __init__(self, metadata_reader, messenger: IMessenger):
        self._metadata_reader = metadata_reader
        self._messenger = messenger

    def print_backup_history(self, limit: int = 10):
        history = self._metadata_reader.get_backup_history(limit)
        if not history:
            self._messenger.warning("No backup history found")
            return
        
        self._messenger.info(f"\n{'='*80}")
        self._messenger.info(f"Recent Backup History (last {limit})")
        self._messenger.info(f"{'='*80}")
        
        for backup in history:
            status_color = "success" if backup.get("status") == "completed" else "error"
            print(f"\nID: {backup.get('id')}")
            print(f"Type: {backup.get('type')}")
            print(f"Status: ", end="")
            if status_color == "success":
                self._messenger.success(backup.get('status'))
            else:
                self._messenger.error(backup.get('status'))
            print(f"Started: {backup.get('timestamp_start')}")
            print(f"Duration: {backup.get('duration_seconds', 0):.2f}s")
            stats = backup.get('statistics', {})
            if stats:
                size_mb = stats.get('total_size_bytes', 0) / 1024 / 1024
                print(f"Tables: {stats.get('total_tables', 0)}")
                print(f"Rows: {stats.get('total_rows_processed', 0)}")
                print(f"Size: {size_mb:.2f} MB")
        self._messenger.info(f"\n{'='*80}\n")
    