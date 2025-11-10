import os
import sys
import subprocess
from pathlib import Path
from console_utils import get_messenger

def validate_mysql_login_path(login_path: str) -> bool:
    """Validate that MySQL login-path exists in mysql_config_editor"""
    messenger = get_messenger()
    
    try:
        result = subprocess.run(
            ["mysql_config_editor", "print", "--all"],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode != 0:
            messenger.error("mysql_config_editor not found or failed to run")
            messenger.info("Install MySQL client tools: brew install mysql-client (macOS) or apt-get install mysql-client (Linux)")
            return False
        
        if f"[{login_path}]" in result.stdout:
            messenger.success(f"Login-path '{login_path}' found in mysql_config_editor")
            return True
        else:
            messenger.error(f"Login-path '{login_path}' not found in mysql_config_editor")
            messenger.info(f"Create it with: mysql_config_editor set --login-path={login_path} --host=localhost --user=root --password")
            return False
    
    except FileNotFoundError:
        messenger.error("mysql_config_editor not found in PATH")
        messenger.info("Install MySQL client tools or use --config manual/file as fallback")
        return False


def validate_mysql_connection_with_login_path(login_path: str, database: str = None) -> bool:
    """
    Validate MySQL connection using login-path by attempting actual connection.
    This is more reliable than just checking if login-path exists.
    
    Args:
        login_path: MySQL login-path name (e.g., 'xtrabackup')
        database: Optional database name to test connection with
        
    Returns:
        bool: True if connection successful, False otherwise
    """
    messenger = get_messenger()
    
    try:
        cmd = ["mysql", f"--login-path={login_path}", "-e", "SELECT 1;"]
        if database:
            cmd.append(database)
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10
        )
        
        if result.returncode == 0:
            messenger.success(f"✓ MySQL connection test successful with login-path '{login_path}'")
            return True
        else:
            messenger.error(f"✗ MySQL connection test failed with login-path '{login_path}'")
            messenger.error(f"Error: {result.stderr.strip()}")
            messenger.info("Check your login-path credentials:")
            messenger.info(f"  mysql_config_editor print --login-path={login_path}")
            return False
            
    except subprocess.TimeoutExpired:
        messenger.error(f"✗ MySQL connection timeout with login-path '{login_path}'")
        messenger.info("Check if MySQL server is running and accessible")
        return False
    except FileNotFoundError:
        messenger.error("✗ mysql command not found in PATH")
        messenger.info("Install MySQL client: brew install mysql-client (macOS)")
        return False
    except Exception as e:
        messenger.error(f"✗ Connection test failed: {e}")
        return False


def validate_postgres_pgpass(host: str, port: str, database: str, user: str) -> bool:
    """Validate that PostgreSQL .pgpass file exists and contains matching entry"""
    messenger = get_messenger()
    
    pgpass_path = Path.home() / ".pgpass"
    
    if not pgpass_path.exists():
        messenger.warning(f".pgpass file not found at {pgpass_path}")
        messenger.info("Create it with: echo 'hostname:port:database:username:password' >> ~/.pgpass")
        messenger.info("Then run: chmod 0600 ~/.pgpass")
        return False
    
    # Check permissions (must be 0600)
    stat_info = pgpass_path.stat()
    if stat_info.st_mode & 0o777 != 0o600:
        messenger.error(f".pgpass has incorrect permissions: {oct(stat_info.st_mode & 0o777)}")
        messenger.info(f"Fix with: chmod 0600 {pgpass_path}")
        return False
    
    with open(pgpass_path, 'r') as f:
        entries = f.readlines()
    
    for entry in entries:
        entry = entry.strip()
        if entry and not entry.startswith('#'):
            parts = entry.split(':')
            if len(parts) == 5:
                e_host, e_port, e_db, e_user, _ = parts
                if (e_host in [host, '*'] and 
                    e_port in [port, '*'] and 
                    e_db in [database, '*'] and 
                    e_user in [user, '*']):
                    messenger.success(f"Found matching .pgpass entry for {user}@{host}:{port}/{database}")
                    return True
    
    messenger.warning(f"No matching .pgpass entry found for {user}@{host}:{port}/{database}")
    messenger.info(f"Add entry: echo '{host}:{port}:{database}:{user}:<password>' >> ~/.pgpass")
    return False


def validate_postgres_connection_with_pgpass(host: str, port: str, database: str, user: str) -> bool:
    """
    Validate PostgreSQL connection using .pgpass by attempting actual connection.
    This is more reliable than just checking if .pgpass entry exists.
    
    Args:
        host: PostgreSQL host
        port: PostgreSQL port
        database: Database name
        user: Username
        
    Returns:
        bool: True if connection successful, False otherwise
    """
    messenger = get_messenger()
    
    try:
        result = subprocess.run(
            [
                "psql",
                "-h", host,
                "-p", str(port),
                "-U", user,
                "-d", database,
                "-c", "SELECT 1;"
            ],
            capture_output=True,
            text=True,
            timeout=10,
            env=os.environ.copy()  # Don't set PGPASSWORD
        )
        
        if result.returncode == 0:
            messenger.success(f"✓ PostgreSQL connection test successful for {user}@{host}:{port}/{database}")
            return True
        else:
            messenger.error(f"✗ PostgreSQL connection test failed")
            messenger.error(f"Error: {result.stderr.strip()}")
            messenger.info("Check your .pgpass credentials:")
            messenger.info(f"  cat ~/.pgpass | grep '{host}:{port}'")
            return False
            
    except subprocess.TimeoutExpired:
        messenger.error(f"✗ PostgreSQL connection timeout")
        messenger.info("Check if PostgreSQL server is running and accessible")
        return False
    except FileNotFoundError:
        messenger.error("✗ psql command not found in PATH")
        messenger.info("Install PostgreSQL client: brew install postgresql (macOS)")
        return False
    except Exception as e:
        messenger.error(f"✗ Connection test failed: {e}")
        return False


def get_mysql_socket_from_server(login_path: str) -> str:
    """Try to get socket path from MySQL server"""
    try:
        result = subprocess.run(
            ["mysql", f"--login-path={login_path}", "-e", "SELECT @@socket;"],
            capture_output=True,
            text=True,
            check=False,
            timeout=5
        )
        
        if result.returncode == 0:
            lines = result.stdout.strip().split('\n')
            if len(lines) >= 2:
                socket_path = lines[1].strip()
                if socket_path and Path(socket_path).exists():
                    return socket_path
    except Exception:
        pass
    
    return ""


def validate_profile_config(args, parser):
    """
    Validate configuration using secure credential profiles.
    
    MySQL: Uses mysql_config_editor login-path (stored in ~/.mylogin.cnf)
    PostgreSQL: Uses ~/.pgpass file
    
    Args:
        args: Parsed command line arguments
        parser: ArgumentParser instance for error reporting
        
    Returns:
        dict: Configuration dictionary with profile-based credentials
    """
    messenger = get_messenger()
    dbname = args.database
    
    messenger.section_header("🔐 Secure Profile Configuration")
    
    if args.db == "mysql":
        default_login_path = "xtrabackup"
        login_path = input(f"Enter MySQL login-path (default: {default_login_path}): ").strip() or default_login_path
        
        if not validate_mysql_login_path(login_path):
            messenger.error("MySQL login-path validation failed")
            sys.exit(1)
        
        if not validate_mysql_connection_with_login_path(login_path, dbname):
            messenger.error("MySQL connection validation failed")
            sys.exit(1)
        
        socket_path = input("MySQL socket path (press Enter to skip): ").strip()
        
        if not socket_path:
            messenger.info("Attempting to detect socket from server...")
            socket_path = get_mysql_socket_from_server(login_path)
            if socket_path:
                messenger.success(f"Detected socket: {socket_path}")
        
        host_override = input("Host override (press Enter to use login-path default): ").strip()
        port_override = input("Port override (press Enter to use login-path default): ").strip()
        
        return {
            'type': 'mysql_profile',
            'login_path': login_path,
            'socket': socket_path,
            'host': host_override or None,
            'port': int(port_override) if port_override else None,
            'dbname': dbname,
            'user': None,  
            'password': None 
        }
    
    elif args.db == "postgres":
        messenger.info("PostgreSQL uses ~/.pgpass for secure credential storage")
        
        host = input("Host (default: localhost): ").strip() or "localhost"
        port = input("Port (default: 5432): ").strip() or "5432"
        user = input("Username: ").strip()
        
        if not user:
            messenger.error("Username is required for PostgreSQL")
            sys.exit(1)
        
        if not validate_postgres_pgpass(host, port, dbname, user):
            messenger.warning("PostgreSQL .pgpass validation failed, but continuing...")
            messenger.info("Connection may fail if credentials are not properly configured")
        
        if not validate_postgres_connection_with_pgpass(host, port, dbname, user):
            messenger.error("PostgreSQL connection validation failed")
            sys.exit(1)
        
        return {
            'type': 'postgres_profile',
            'host': host,
            'port': port,
            'user': user,
            'password': None,  # Will be read from .pgpass by psycopg2/pg_basebackup
            'dbname': dbname
        }
    
    else:
        messenger.error(f"Unsupported database type: {args.db}")
        sys.exit(1)


def validate_manual_config(args, parser):
    """
    Validate manual configuration from command line arguments.
    
    Args:
        args: Parsed command line arguments
        parser: ArgumentParser instance for error reporting
        
    Returns:
        dict: Configuration dictionary with host, port, user, password, dbname
    """
    messenger = get_messenger()
    host = args.host
    port = args.port
    user = args.user
    password = args.password or ""
    dbname = args.database
    
    if not all([host, port, user]):
        parser.error("--config manual requires --host, --port, and --user")
    
    if not password:
        messenger.warning("No password provided. Connection may fail.")
    
    return {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'dbname': dbname
    }


def validate_file_config(args):
    """
    Validate configuration from environment variables (.env file).
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        dict: Configuration dictionary with host, port, user, password, dbname
        
    Raises:
        SystemExit: If required environment variables are missing
    """
    messenger = get_messenger()
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD", "")
    dbname = args.database

    missing_vars = []
    if not host:
        missing_vars.append("DB_HOST")
    if not port:
        missing_vars.append("DB_PORT")
    if not user:
        missing_vars.append("DB_USER")
        
    if missing_vars:
        messenger.error("Missing required environment variables in .env file:")
        for var in missing_vars:
            messenger.error(f"  - {var}")
        sys.exit(1)
    
    if not password:
        messenger.warning("DB_PASSWORD not set in .env. Connection may fail.")
    
    return {
        'host': host,
        'port': port,
        'user': user,
        'password': password,
        'dbname': dbname
    }


def validate_config(args, parser):
    """
    Main configuration validation function that delegates to specific validators.
    
    Args:
        args: Parsed command line arguments
        parser: ArgumentParser instance for error reporting
        
    Returns:
        dict: Configuration dictionary with host, port, user, password, dbname
    """
    if args.config == "manual":
        return validate_manual_config(args, parser)
    elif args.config == "file":
        return validate_file_config(args)
    elif args.config == "profile":
        return validate_profile_config(args, parser)
    else:
        parser.error(f"Unsupported config type: {args.config}")