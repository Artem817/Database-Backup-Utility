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
from cli.validateconfig import validate_config
from console_utils import get_messenger, configure_messenger

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
    """
)

parser.add_argument(
    "command", 
    choices=["backup"], 
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
    required=True, 
    choices=["local", "cloud"], 
    help="Storage type (currently only local is implemented)"
)

parser.add_argument(
    "--config", 
    required=True, 
    choices=["manual", "file"], 
    help="Configuration source: 'manual' for CLI args or 'file' for .env"
)

parser.add_argument("--host", help="Database host address")
parser.add_argument("--port", help="Database port (default: 5432 for postgres)")
parser.add_argument("--user", help="Database username")
parser.add_argument("--password", help="Database password")

args = parser.parse_args()

db_client = None
messenger = get_messenger()

try:
    config = validate_config(args, parser)
    host = config['host']
    port = config['port']
    user = config['user']
    password = config['password']
    dbname = config['dbname']

    if not port:
        port = "5432" if args.db == "postgres" else "3306"

    if args.storage == "cloud":
        messenger.warning("Cloud storage is not yet implemented. Use --storage local instead.")
        sys.exit(EXIT_FAILURE)
        
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
    
    asyncio.run(interactive_console(db_client=db_client, dbname=dbname, user=user))

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