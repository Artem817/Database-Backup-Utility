from typing import Optional, AnyStr, Any
from pathlib import Path
import json

from console_utils import get_messenger
from custom_logging import BackupCatalog, BackupLogger
from services.backup.metadata import BackupMetadataReader, BackupHistoryService


class BackupCatalogMixin:
    _messenger : Any
    _logger: BackupLogger
    _database: str

    def get_last_backup_path(self) -> str | None:
        catalog = BackupCatalog()
        return catalog.get_last_backup().get("backup_location") if catalog.get_last_backup() else None

    def print_backup_history(self, limit: int = 10):
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        history_service = BackupHistoryService(metadata_reader, self._messenger)
        history_service.print_backup_history(limit)

    def get_backup_history(self, limit: int = 10) -> list:
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        return metadata_reader.get_backup_history(limit)

    def get_last_full_backup_timestamp(self) -> str | None:
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        return metadata_reader.get_last_full_backup_timestamp()

    def get_table_names_from_last_full_backup(self) -> list[str]:
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        return metadata_reader.get_table_names_from_last_full_backup()

    def get_output_path_from_last_full_backup(self) -> str | None:
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        return metadata_reader.get_output_path_from_last_full_backup()

    def _create_backup_structure(self, base_path: Path, backup_id: str, back_up_time: str) -> dict:
        """
        Creates organized backup directory structure:
        /base_path/
            └─ {database}/
                └─ {backup_id}/
                    ├─ metadata.json
                    └─ differentials/
                        └─ chain.json
        """
        base_path = Path(base_path) if isinstance(base_path, str) else base_path
        
        database_dir = base_path / self._database
        database_dir.mkdir(parents=True, exist_ok=True)
        
        backup_root = database_dir / backup_id
        backup_root.mkdir(parents=True, exist_ok=True)
        
        differentials_dir = backup_root / "differentials"
        differentials_dir.mkdir(parents=True, exist_ok=True)
        
        chain_file = differentials_dir / "chain.json"
        if not chain_file.exists():
            chain_data = {
                "full_backup_id": backup_id,
                "full_backup_time": back_up_time,
                "differentials": []
            }
            with open(chain_file, 'w') as f:
                json.dump(chain_data, f, indent=2)
        
        metadata_file = backup_root / "metadata.json"
        
        return {
            "backup_root": backup_root,
            "database_dir": database_dir,
            "differentials_dir": differentials_dir,
            "chain_file": chain_file,
            "metadata_file": metadata_file
        }