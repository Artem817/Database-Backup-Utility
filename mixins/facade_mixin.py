from pathlib import Path
from typing import Any

from custom_logging import BackupLogger
from services.backup.core import CompressionService
from services.backup.exporters import TableExporter, SchemaExporter
from services.execution.executor import QueryExecutor
from services.execution.exporter import QueryResultExporter


class ServiceFacadeMixin:
    _messenger : Any
    _logger: BackupLogger
    _database: str

    def get_tables(self):
        table_exporter = TableExporter(self, self._logger, self._messenger)
        return table_exporter.get_tables()

    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.table_exists(table_name, schema)

    def database_schema(self, output_path: Path) -> str | None:
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.export_schema(output_path)

    def csv_fragmental_backup(self, rows, outpath, query: str = None):
        query_result_exporter = QueryResultExporter(self._logger, self._messenger, self._database)
        return query_result_exporter.export_csv(rows, outpath, query)

    def extract_sql_query(self, query: str, outpath):
        query_executor = QueryExecutor(self, self._logger, self._messenger)
        query_result_exporter = QueryResultExporter(self._logger, self._messenger, self._database)
        return query_executor.extract_sql_query(query, outpath, query_result_exporter)

    def compress_backup(self, path):
        compression_service = CompressionService(self._logger, self._messenger)
        return compression_service.compress_backup(path)

    def get_table_schema(self, table_name: str, schema: str = "public"):
        schema_exporter = SchemaExporter(self, self._logger, self._messenger)
        return schema_exporter.get_table_schema(table_name, schema)