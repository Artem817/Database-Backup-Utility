from abc import ABC, abstractmethod

class DatabaseClient(ABC):
    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def execute_query(self, query):
        pass

    @abstractmethod
    def backup_full(self, outpath: str) -> bool:
        """Create full database backup with zstd compression"""
        pass

    @abstractmethod
    def validate_connection(self):
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
