from console_utils import get_messenger
from custom_logging import BackupLogger

class ConnectionConfigMixin:
    def __init__(self, host, database, user, password, _compressing_level = 4, logger: BackupLogger = None, messenger=None, port=5432,
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
        self._compressing_level = _compressing_level #TODO CLI REQUEST
        
        # Initialize optional attributes for different database types
        self._login_path = None
        self._socket = None

    @property
    def database_name(self):
        return self._database
    @property
    def connection_params(self):
        """Return connection parameters for backup utilities"""
        params = {
            'host': self._host,
            'port': self._port,
            'user': self._user,
            'password': self._password,
            'database': self._database
        }
        
        # Only include optional params if they exist
        if hasattr(self, '_login_path') and self._login_path:
            params['login_path'] = self._login_path
        
        if hasattr(self, '_socket') and self._socket:
            params['socket'] = self._socket
            
        if hasattr(self, '_use_pgpass') and self._use_pgpass:
            params['use_pgpass'] = self._use_pgpass
        
        return params

    def get_connection_params(self):
        return self.connection_params

