from typing import Optional, AnyStr, Any

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