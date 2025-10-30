from console_utils import get_messenger
from custom_logging import BackupLogger

class ConnectionConfigMixin:
    def __init__(self, host, database, user, password, logger: BackupLogger = None, messenger=None, port=5432,
                     utility_version="1.0.0"):
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port
        self._utility_version = utility_version
        self._database_version = None
        self.compress: bool = False
        self._logger = logger if logger is not None else BackupLogger(name=f"backup_{database}",
                                                                      log_file=f"backup_{database}.log")
        self._messenger = messenger if messenger is not None else get_messenger()

    @property
    def database_name(self):
        return self._database

    @property
    def connection_params(self):
        return {"host": self._host, "user": self._user, "database": self._database, "port": self._port,
                "password": self._password}

    def get_connection_params(self):
        return self.connection_params

