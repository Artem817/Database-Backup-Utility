import os
import sys
import subprocess
from pathlib import Path
from getpass import getpass
from console_utils import get_messenger


class MySQLLoginPathManager:
    """Manages MySQL login-path profiles using mysql_config_editor"""
    
    def __init__(self):
        self._messenger = get_messenger()
        self._mylogin_file = Path.home() / ".mylogin.cnf"
    
    def check_mysql_config_editor_available(self) -> bool:
        """Check if mysql_config_editor is available"""
        try:
            result = subprocess.run(
                ["mysql_config_editor", "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False
    
    def validate_login_path(self, login_path: str) -> bool:
        """Check if login-path exists in mysql_config_editor"""
        try:
            result = subprocess.run(
                ["mysql_config_editor", "print", "--all"],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            if result.returncode != 0:
                return False
            
            # Check if the login-path section exists
            return f"[{login_path}]" in result.stdout
        except Exception as e:
            self._messenger.error(f"Failed to validate login-path: {e}")
            return False
    
    def create_login_path(self, login_path: str, host: str, user: str, socket: str = None) -> bool:
        """Interactive creation of MySQL login-path"""
        self._messenger.info(f"Creating login-path profile: {login_path}")
        
        cmd = [
            "mysql_config_editor",
            "set",
            f"--login-path={login_path}",
            f"--host={host}",
            f"--user={user}",
            "--password"
        ]
        
        if socket:
            cmd.append(f"--socket={socket}")
        
        try:
            self._messenger.info("You will be prompted to enter the password...")
            result = subprocess.run(cmd)
            
            if result.returncode == 0:
                self._messenger.success(f"Login-path '{login_path}' created successfully!")
                return True
            else:
                self._messenger.error(f"Failed to create login-path")
                return False
        except Exception as e:
            self._messenger.error(f"Error creating login-path: {e}")
            return False
    
    def list_login_paths(self):
        """Display all configured login-paths"""
        try:
            result = subprocess.run(
                ["mysql_config_editor", "print", "--all"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0 and result.stdout.strip():
                self._messenger.info("Configured login-paths:")
                print(result.stdout)
            else:
                self._messenger.warning("No login-paths configured")
        except Exception as e:
            self._messenger.error(f"Failed to list login-paths: {e}")
    
    def check_mylogin_permissions(self) -> bool:
        """Verify ~/.mylogin.cnf has correct permissions (0600)"""
        if not self._mylogin_file.exists():
            return True  
        
        import stat
        st = self._mylogin_file.stat()
        mode = st.st_mode & 0o777
        
        if mode != 0o600:
            self._messenger.warning(
                f"~/.mylogin.cnf has incorrect permissions: {oct(mode)}"
            )
            self._messenger.info("Fixing permissions to 0600...")
            try:
                self._mylogin_file.chmod(0o600)
                return True
            except Exception as e:
                self._messenger.error(f"Failed to fix permissions: {e}")
                return False
        
        return True


class PostgreSQLPgPassManager:
    """Manages PostgreSQL password file ~/.pgpass"""
    
    def __init__(self):
        self._messenger = get_messenger()
        self._pgpass_file = Path.home() / ".pgpass"
    
    def validate_pgpass_entry(self, host: str, port: int, database: str, user: str) -> bool:
        """Check if .pgpass entry exists for given credentials"""
        if not self._pgpass_file.exists():
            return False
        
        try:
            with open(self._pgpass_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    
                    parts = line.split(':')
                    if len(parts) != 5:
                        continue
                    
                    p_host, p_port, p_db, p_user, _ = parts
                    
                    # Match with wildcards
                    if (p_host in [host, '*'] and
                        p_port in [str(port), '*'] and
                        p_db in [database, '*'] and
                        p_user in [user, '*']):
                        return True
            
            return False
        except Exception as e:
            self._messenger.error(f"Failed to read .pgpass: {e}")
            return False
    
    def create_pgpass_entry(self, host: str, port: int, database: str, user: str) -> bool:
        """Interactive creation of .pgpass entry"""
        self._messenger.info("Creating .pgpass entry for PostgreSQL")
        
        password = getpass("Enter PostgreSQL password: ")
        
        if not password:
            self._messenger.error("Password cannot be empty")
            return False
        
        entry = f"{host}:{port}:{database}:{user}:{password}\n"
        
        try:
            if not self._pgpass_file.exists():
                self._pgpass_file.touch(mode=0o600)
            else:
                self._pgpass_file.chmod(0o600)
            
            with open(self._pgpass_file, 'a') as f:
                f.write(entry)
            
            self._messenger.success(f".pgpass entry created successfully!")
            return True
        except Exception as e:
            self._messenger.error(f"Failed to create .pgpass entry: {e}")
            return False
    
    def check_pgpass_permissions(self) -> bool:
        """Verify ~/.pgpass has correct permissions (0600)"""
        if not self._pgpass_file.exists():
            return True 
        
        import stat
        st = self._pgpass_file.stat()
        mode = st.st_mode & 0o777
        
        if mode != 0o600:
            self._messenger.warning(
                f"~/.pgpass has incorrect permissions: {oct(mode)}"
            )
            self._messenger.info("Fixing permissions to 0600...")
            try:
                self._pgpass_file.chmod(0o600)
                return True
            except Exception as e:
                self._messenger.error(f"Failed to fix permissions: {e}")
                return False
        
        return True


def validate_login_path_config(args, parser):
    """
    Validate configuration using encrypted login-path profiles.
    
    For MySQL: uses mysql_config_editor login-path
    For PostgreSQL: uses ~/.pgpass
    """
    messenger = get_messenger()
    
    if args.db == "mysql":
        return validate_mysql_login_path(args, parser)
    elif args.db == "postgres":
        return validate_postgres_pgpass(args, parser)
    else:
        parser.error(f"Unsupported database type: {args.db}")


def validate_mysql_login_path(args, parser):
    """Validate MySQL configuration using login-path"""
    messenger = get_messenger()
    mysql_manager = MySQLLoginPathManager()
    
    if not mysql_manager.check_mysql_config_editor_available():
        messenger.error("mysql_config_editor not found!")
        messenger.info("Please install MySQL client tools:")
        messenger.info("  macOS: brew install mysql-client")
        messenger.info("  Ubuntu/Debian: apt-get install mysql-client")
        messenger.info("  RHEL/CentOS: yum install mysql")
        sys.exit(1)
    
    if not mysql_manager.check_mylogin_permissions():
        messenger.error("Cannot proceed with incorrect .mylogin.cnf permissions")
        sys.exit(1)
    
    login_path = args.login_path or input(
        "Enter login-path name (default: xtrabackup): "
    ).strip() or "xtrabackup"
    
    if not mysql_manager.validate_login_path(login_path):
        messenger.warning(f"Login-path '{login_path}' not found")
        messenger.info("Available login-paths:")
        mysql_manager.list_login_paths()
        
        create = input(f"\nCreate new login-path '{login_path}'? [Y/n]: ").strip().lower()
        if create in ['', 'y', 'yes']:
            host = args.host or input("Enter host (default: localhost): ").strip() or "localhost"
            user = args.user or input("Enter username (default: root): ").strip() or "root"
            socket = input("Enter socket path (or press Enter to skip): ").strip() or None
            
            if not mysql_manager.create_login_path(login_path, host, user, socket):
                messenger.error("Failed to create login-path")
                sys.exit(1)
        else:
            messenger.error("Cannot proceed without valid login-path")
            sys.exit(1)
    
    dbname = args.database
    if not dbname:
        parser.error("--database is required")
    
    socket = getattr(args, 'socket', None)
    
    messenger.success(f"Using MySQL login-path: {login_path}")
    
    return {
        'login_path': login_path,
        'dbname': dbname,
        'socket': socket,
        'db_type': 'mysql'
    }


def validate_postgres_pgpass(args, parser):
    """Validate PostgreSQL configuration using .pgpass"""
    messenger = get_messenger()
    postgres_manager = PostgreSQLPgPassManager()
    
    if not postgres_manager.check_pgpass_permissions():
        messenger.error("Cannot proceed with incorrect .pgpass permissions")
        sys.exit(1)
    
    host = args.host or input("Enter host (default: localhost): ").strip() or "localhost"
    port = int(args.port or input("Enter port (default: 5432): ").strip() or "5432")
    user = args.user or input("Enter username: ").strip()
    dbname = args.database
    
    if not user:
        parser.error("Username is required for PostgreSQL")
    if not dbname:
        parser.error("--database is required")
    
    if not postgres_manager.validate_pgpass_entry(host, port, dbname, user):
        messenger.warning(f"No .pgpass entry found for {user}@{host}:{port}/{dbname}")
        
        create = input("Create .pgpass entry? [Y/n]: ").strip().lower()
        if create in ['', 'y', 'yes']:
            if not postgres_manager.create_pgpass_entry(host, port, dbname, user):
                messenger.error("Failed to create .pgpass entry")
                sys.exit(1)
        else:
            messenger.error("Cannot proceed without .pgpass entry")
            sys.exit(1)
    
    messenger.success(f"Using PostgreSQL .pgpass authentication for {user}@{host}")
    
    return {
        'host': host,
        'port': port,
        'user': user,
        'dbname': dbname,
        'password': '',  # Will be read from .pgpass by psycopg2
        'db_type': 'postgres'
    }
