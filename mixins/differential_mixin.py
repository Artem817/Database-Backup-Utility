from typing import Any
from custom_logging import BackupLogger, BackupCatalog
from services.backup.core import DifferentialBackupService
from services.backup.metadata import BackupMetadataReader


class DifferentialBackupMixin:
    """
    Provides methods for incremental backup (WAL-based).
    PostgreSQL: pg_basebackup --incremental
    MySQL: xtrabackup --incremental-basedir
    """
    _messenger: Any
    _logger: BackupLogger
    _database: str

    def perform_differential_backup(self, metadata_reader: BackupMetadataReader):
        """Performs incremental backup using native database utilities"""
        diff_service = DifferentialBackupService(self, self._logger, self._messenger)
        return diff_service.perform_differential_backup(metadata_reader)
