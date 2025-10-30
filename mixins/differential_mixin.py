from datetime import datetime
from pathlib import Path
from typing import Any

from custom_logging import BackupLogger, BackupCatalog
from services.backup.core import DifferentialBackupService
from services.backup.exporters import SchemaExporter, TableExporter
from services.backup.metadata import BackupMetadataReader


class DifferentialBackupMixin:
    """
    Provides methods for differential backup.
    Expects 'self' to have: _messenger, _logger, _database
    and implements IConnectionProvider (to pass 'self' to services).
    """
    _messenger: Any
    _logger: BackupLogger
    _database: str

    def perform_differential_backup(self, basis: str, tables: list = None):
        catalog = BackupCatalog()
        metadata_reader = BackupMetadataReader(catalog, self._messenger, self._logger, self._database)
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        table_exporter = TableExporter(self, self._logger, self._messenger)

        diff_service = DifferentialBackupService(self, self._logger, self._messenger, schema_exporter, table_exporter)
        return diff_service.perform_differential_backup(basis, metadata_reader, tables)

    def export_diff_table(self, tables, last_backup_time: datetime, outpath: Path, basis: str) -> dict:
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        table_exporter = TableExporter(self, self._logger, self._messenger)
        diff_service = DifferentialBackupService(self, self._logger, self._messenger, schema_exporter, table_exporter)
        return diff_service.export_diff_table(tables, last_backup_time, outpath, basis)

    def get_max_updated_at(self, table_name: str, schema: str, column: str):
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        table_exporter = TableExporter(self, self._logger, self._messenger)
        diff_service = DifferentialBackupService(self, self._logger, self._messenger, schema_exporter, table_exporter)
        return diff_service.get_max_updated_at(table_name, schema, column)