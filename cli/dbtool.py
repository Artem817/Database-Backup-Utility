import argparse
from dotenv import load_dotenv
import os
import asyncio
import sys
from colorama import init

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from clients.postgres_client import PostgresClient
from clients.mysql_client import MysqlClient  
from cli.interaction import interactive_console
from cli.postgres_wal_config import PostgresWalArchiveConfig
from cli.validateconfig import validate_config
from console_utils import get_messenger, configure_messenger
from custom_logging import BackupLogger
from services.restore.postgres_restore import PostgresRestoreService

EXIT_FAILURE = 1

init(autoreset=True)
load_dotenv()

parser = argparse.ArgumentParser(
    description="Database Backup Utility - A tool for backing up and managing databases",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # PostgreSQL  backup using .env file
  python cli/dbtool.py backup --db postgres --database testdb --storage local --config file
  
  # MySQL backup using .env file  
  python cli/dbtool.py backup --db mysql --database testdb --storage local --config file
  
  # Using manual configuration
  python cli/dbtool.py backup --db postgres --database testdb --storage local --config manual \\
    --host localhost --port 5432 --user admin --password secret

  # PostgreSQL restore from a full backup directory
  python cli/dbtool.py restore --db postgres --database postgres \\
    --backup-path /backups/full_pg_20260427_120000_abcd \\
    --data-dir /tmp/postgres-restore --archive-dir /tmp/pg_archive
    """
)

parser.add_argument(
    "command", 
    choices=["backup", "restore"], 
    help="Command to execute"
)

parser.add_argument(
    "--db", 
    required=True, 
    choices=["postgres", "mysql"], 
    help="Database type: postgres or mysql"
)

parser.add_argument(
    "--database", 
    required=True, 
    help="Name of the database"
)

parser.add_argument(
    "--storage", 
    choices=["local", "cloud"], 
    help="Storage type (currently only local is implemented)"
)

parser.add_argument(
    "--config", 
    choices=["manual", "file", "profile"], 
    help="Configuration source: 'manual' for CLI args, 'file' for .env, 'profile' for encrypted login-path"
)

parser.add_argument("--host", help="Database host address")
parser.add_argument("--port", help="Database port (default: 5432 for postgres)")
parser.add_argument("--user", help="Database username")
parser.add_argument("--password", help="Database password")
parser.add_argument("--backup-path", help="PostgreSQL full backup directory or .tar.zst archive to restore")
parser.add_argument("--data-dir", help="New PostgreSQL data directory for restore")
parser.add_argument("--archive-dir", help="Path to archived WAL files for PostgreSQL restore")
parser.add_argument(
    "--restore-port",
    type=int,
    default=55432,
    help="Port for the restored PostgreSQL instance (default: 55432)"
)
parser.add_argument(
    "--verify-user",
    default="postgres",
    help="Database user for post-restore SELECT verification (default: postgres)"
)
parser.add_argument(
    "--verify-query",
    default="SELECT 1;",
    help="SQL query used to verify the restored instance (default: SELECT 1;)"
)
parser.add_argument(
    "--pg-bin-dir",
    help="Directory containing pg_ctl and psql when PostgreSQL binaries are not in PATH"
)

parser.add_argument(
    "--single-archive",
    type=lambda x: x.lower() in ['true', 't', 'yes', 'y', '1'],
    default=True,
    help="Compress entire backup into single .tar.zst archive (default: True, faster for AWS S3)"
)

args = parser.parse_args()

db_client = None
messenger = get_messenger()

try:
    if args.command == "restore":
        if args.db != "postgres":
            parser.error("Restore is currently implemented only for --db postgres")
        if not args.backup_path:
            parser.error("--backup-path is required for restore")
        if not args.data_dir:
            parser.error("--data-dir is required for restore")

        restore_logger = BackupLogger(
            name=f"restore_{args.database}",
            log_file=f"restore_{args.database}.log"
        )
        configure_messenger(logger=restore_logger.logger, enable_colors=True)
        messenger = get_messenger()

        archive_dir = args.archive_dir
        if not archive_dir:
            archive_dir = PostgresWalArchiveConfig().get_archive_directory()
            if archive_dir:
                messenger.info(f"Using saved WAL archive directory: {archive_dir}")

        if not archive_dir:
            parser.error(
                "--archive-dir is required for restore when no saved PostgreSQL WAL archive directory exists"
            )

        restore_service = PostgresRestoreService(
            backup_path=args.backup_path,
            data_dir=args.data_dir,
            archive_dir=archive_dir,
            restore_port=args.restore_port,
            verify_database=args.database,
            verify_user=args.verify_user,
            verify_query=args.verify_query,
            pg_bin_dir=args.pg_bin_dir,
            logger=restore_logger,
            messenger=messenger,
        )

        if not restore_service.restore():
            sys.exit(EXIT_FAILURE)

        sys.exit(0)

    if not args.storage:
        parser.error("--storage is required for backup")
    if not args.config:
        parser.error("--config is required for backup")

    config = validate_config(args, parser)
    
    config_type = config.get('type')
    
    user = None
    dbname = config.get('dbname', args.database)
    
    if config_type == 'mysql_profile':
        # MySQL login-path configuration
        login_path = config['login_path']
        host = config.get('host')
        port = config.get('port')
        user = config.get('user') or 'root' 
        
        messenger.section_header("Configuration (MySQL Login-Path)")
        messenger.config_item("Database Type", "MySQL")
        messenger.config_item("Login-Path", login_path)
        messenger.config_item("Database", dbname)
        if host:
            messenger.config_item("Host Override", host)
        if port:
            messenger.config_item("Port Override", port)
        if config.get('socket'):
            messenger.config_item("Socket", config['socket'])
        messenger.info("")
        
        messenger.info("Initializing MySQL client with login-path...")
        db_client = MysqlClient(
            host=host or 'localhost',
            database=dbname,
            user='',  # Will be read from login-path
            password='',  
            port=int(port) if port else 3306,
            login_path=login_path,
            socket=config.get('socket')
        )
        
    elif config_type == 'postgres_profile':
        host = config['host']
        port = config['port']
        user = config['user']
        
        messenger.section_header("Configuration (PostgreSQL .pgpass)")
        messenger.config_item("Database Type", "PostgreSQL")
        messenger.config_item("Host", host)
        messenger.config_item("Port", port)
        messenger.config_item("User", user)
        messenger.config_item("Database", dbname)
        messenger.config_item("Password Source", "~/.pgpass")
        messenger.info("")
        
        messenger.info("Initializing PostgreSQL client with .pgpass...")
        db_client = PostgresClient(
            host=host,
            database=dbname,
            user=user,
            password='',  # Will be read from .pgpass by psycopg2
            port=int(port),
            use_pgpass=True
        )
        
    else:
        host = config['host']
        port = config['port']
        user = config['user']
        password = config['password']

        if not port:
            port = "5432" if args.db == "postgres" else "3306"

        messenger.section_header("Configuration")
        messenger.config_item("Database Type", args.db)
        messenger.config_item("Host", host)
        messenger.config_item("Port", port)
        messenger.config_item("User", user)
        messenger.config_item("Database", dbname)
        messenger.config_item("Password", password, mask_value=True)
        messenger.config_item("Storage", args.storage)
        messenger.info("")

        messenger.info("Initializing database client...")
        
        if args.db == "postgres":
            db_client = PostgresClient(
                host=host,
                database=dbname,
                user=user,
                password=password,
                port=int(port)
            )
        elif args.db == "mysql":
            db_client = MysqlClient(
                host=host,
                database=dbname,
                user=user,
                password=password,
                port=int(port)
            )
        else:
            messenger.error(f"Unsupported database type: {args.db}")
            sys.exit(EXIT_FAILURE)
    
    configure_messenger(logger=db_client._logger.logger, enable_colors=True)
    messenger = get_messenger() 
    
    messenger.info("Connecting to database...")
    connection = db_client.connect()
    
    if connection is None:
        messenger.error("Failed to establish database connection.")
        sys.exit(EXIT_FAILURE)
    
    if not db_client.validate_connection():
        messenger.error("Connection validation failed.")
        sys.exit(EXIT_FAILURE)
    
    messenger.success("Connection established and validated successfully!\n")
    
    asyncio.run(
        interactive_console(
            db_client=db_client,
            dbname=dbname,
            user=user,
            initial_storage_type=args.storage,
        )
    )

except KeyboardInterrupt:
    messenger.info("\n\nInterrupted by user. Exiting...")
    sys.exit(0)

except Exception as e:
    messenger.critical(str(e))
    import traceback
    traceback.print_exc()
    sys.exit(EXIT_FAILURE)

finally:
    if db_client is not None and db_client.is_connected:
        try:
            db_client.disconnect()
            messenger.success("Database connection closed successfully.")
        except Exception as e:
            messenger.error(f"Error closing connection: {e}")
