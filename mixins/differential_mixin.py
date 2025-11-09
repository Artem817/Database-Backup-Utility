from typing import Any
from custom_logging import BackupLogger, BackupCatalog
from services.backup.core import DifferentialBackupService
from services.backup.metadata import BackupMetadataReader
from services.backup.differential.starategy.postgres_strategy import PostgresDifferentialBackupStrategy
from services.backup.differential.starategy.mysql_strategy import MySQLDifferentialBackupStrategy


class DifferentialBackupMixin:
    """
    Provides methods for incremental backup (WAL-based).
    Automatically selects a strategy based on the database (port) type.
    
    PostgreSQL: WAL-based differential backup
    MySQL: xtrabackup --incremental
    """
    _messenger: Any
    _logger: BackupLogger
    _database: str
    _port: int

    def perform_differential_backup(self, metadata_reader: BackupMetadataReader):
        """Performs differential backup using appropriate strategy for database type"""
        
        if self._port == 3306:  # MySQL
            strategy = MySQLDifferentialBackupStrategy(self, self._logger, self._messenger)
        else:  # PostgreSQL (5432)
            strategy = PostgresDifferentialBackupStrategy(self, self._logger, self._messenger)
        
        diff_service = DifferentialBackupService(self, self._logger, self._messenger, strategy)
        
        return diff_service.perform_differential_backup(metadata_reader)
