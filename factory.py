from abc import ABC, abstractmethod

class DatabaseClient(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def fetch_all(self, query):
        pass

    @abstractmethod
    def fetch_one(self, query):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass

    @abstractmethod
    def table_exists(self, table_name):
        pass

    @abstractmethod
    def export_table(self, tables, outpath, metadata=None):
        pass

    @abstractmethod
    def execute_query(self, query):
        pass

    @abstractmethod
    def get_tables(self):
        pass

    @abstractmethod
    def get_table_schema(self, table_name):
        pass

    @abstractmethod
    def backup_full(self, outpath: str) -> bool:
        """Create full database backup with zstd compression"""
        pass

    @abstractmethod
    def partial_backup(self, tables, outpath, backup_type: str = "partial") -> bool:
        """Create partial backup for specified tables with zstd compression"""
        pass

    @abstractmethod
    def validate_connection(self):
        pass

    @abstractmethod
    def get_database_size(self):
        pass

    @property
    @abstractmethod
    def connection(self):
        pass

    @property
    @abstractmethod
    def database_name(self):
        pass

    @property
    @abstractmethod
    def connection_params(self):
        pass

    @property
    @abstractmethod
    def is_connected(self):
        pass

    # Optional hooks
    def _format_table_name(self, schema, table):
        pass

    def _get_column_info(self, cursor):
        pass

    def _safe_query_execution(self, query):
        pass
