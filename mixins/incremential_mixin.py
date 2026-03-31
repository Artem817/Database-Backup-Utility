# mixins/incremential_mixin.py
from typing import Any
from custom_logging import BackupLogger
from services.backup.metadata import BackupMetadataReader
from services.backup.incremential.postgres_incremental_strategy import (
    PostgresIncrementalBackupStrategy
)

class IncrementialBackupMixin:
    """
    WAL-based incremental backups.
    PostgreSQL: WAL-based incremental
    (Other DBs later)
    """
    _messenger: Any
    _logger: BackupLogger
    _database: str
    _port: int

    def perform_incremental_backup(
        self,
        metadata_reader: BackupMetadataReader,
        outpath: str,
        storage: str = "local",
    ) -> bool:
        if self._port == 5432:
            strategy = PostgresIncrementalBackupStrategy(
                self, self._logger, self._messenger
            )
        else:
            self._messenger.error("Incremental backup is supported only for PostgreSQL for now.")
            return False

        #for v1.0.0, you can call the strategy directly
        return strategy.perform_incremental_backup(metadata_reader, outpath)
