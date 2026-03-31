from typing import Any
from custom_logging import BackupLogger
from services.backup.core import DifferentialBackupService
from services.backup.metadata import BackupMetadataReader
from services.backup.differential.strategy.postgres_strategy import PostgresDifferentialBackupStrategy
from services.backup.differential.strategy.mysql_strategy import MySQLDifferentialBackupStrategy


class DifferentialBackupMixin:
    """
    Provides methods for differential backup.
    Automatically selects a strategy based on the database engine.
    
    PostgreSQL: WAL-based differential backup
    MySQL: xtrabackup --incremental
    """
    _messenger: Any
    _logger: BackupLogger
    _database: str
    _port: int

    def perform_differential_backup(self, metadata_reader: BackupMetadataReader):
        """Performs differential backup using appropriate strategy for database type"""
        database_engine = getattr(self, "database_engine", None) or getattr(
            self, "_database_engine", None
        )

        if database_engine == "mysql":
            strategy = MySQLDifferentialBackupStrategy(self, self._logger, self._messenger)
        elif database_engine == "postgresql":
            strategy = PostgresDifferentialBackupStrategy(self, self._logger, self._messenger)
        else:
            raise ValueError(
                f"Unsupported database engine for differential backup: {database_engine}"
            )
        
        diff_service = DifferentialBackupService(self, self._logger, self._messenger, strategy)
        
        return diff_service.perform_differential_backup(metadata_reader)
