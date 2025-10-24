import argparse
from dotenv import load_dotenv
import os
import asyncio
import sys
from colorama import Fore, Style, init

from postgres_client import PostgresClient
from interaction import interactive_console

init(autoreset=True)
load_dotenv()

parser = argparse.ArgumentParser(
    description="Database Backup Utility - A tool for backing up and managing databases",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    epilog="""
Examples:
  # Using .env file configuration
  python dbtool.py backup --db postgres --database mydb --storage local --config file
  
  # Using manual configuration
  python dptool.py backup --db postgres --database mydb --storage local --config manual \\
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
    help="Database type (currently only postgres is fully implemented)"
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

try:
    if args.config == "manual":
        host = args.host
        port = args.port
        user = args.user
        password = args.password or ""
        dbname = args.database
        
        if not all([host, port, user]):
            parser.error("--config manual requires --host, --port, and --user")
        
        if not password:
            print(Fore.YELLOW + "[WARNING] No password provided. Connection may fail." + Style.RESET_ALL)
        
    elif args.config == "file":
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
            print(Fore.RED + "Error: Missing required environment variables in .env file:" + Style.RESET_ALL)
            for var in missing_vars:
                print(Fore.RED + f"  - {var}" + Style.RESET_ALL)
            sys.exit(1)
        
        if not password:
            print(Fore.YELLOW + "[WARNING] DB_PASSWORD not set in .env. Connection may fail." + Style.RESET_ALL)

    if not port:
        port = "5432" if args.db == "postgres" else "3306"

    if args.db == "mysql":
        print(Fore.YELLOW + "[WARNING] MySQL support is not yet implemented. Only PostgreSQL is currently supported." + Style.RESET_ALL)
        sys.exit(1)
        
    if args.storage == "cloud":
        print(Fore.YELLOW + "[WARNING] Cloud storage is not yet implemented. Use --storage local instead." + Style.RESET_ALL)
        sys.exit(1)
    print(Fore.CYAN + "\n=== Configuration ===" + Style.RESET_ALL)
    print(f"  Database Type: {Fore.GREEN}{args.db}{Style.RESET_ALL}")
    print(f"  Host: {Fore.GREEN}{host}{Style.RESET_ALL}")
    print(f"  Port: {Fore.GREEN}{port}{Style.RESET_ALL}")
    print(f"  User: {Fore.GREEN}{user}{Style.RESET_ALL}")
    print(f"  Database: {Fore.GREEN}{dbname}{Style.RESET_ALL}")
    print(f"  Password: {Fore.GREEN}{'***' if password else '(not set)'}{Style.RESET_ALL}")
    print(f"  Storage: {Fore.GREEN}{args.storage}{Style.RESET_ALL}")
    print()

    print(Fore.YELLOW + "Initializing database client..." + Style.RESET_ALL)
    db_client = PostgresClient(
        host=host,
        database=dbname,
        user=user,
        password=password,
        port=int(port)
    )
    
    print(Fore.YELLOW + "Connecting to database..." + Style.RESET_ALL)
    connection = db_client.connect()
    
    if connection is None:
        print(Fore.RED + "✗ Failed to establish database connection." + Style.RESET_ALL)
        sys.exit(1)
    
    if not db_client.validate_connection():
        print(Fore.RED + "✗ Connection validation failed." + Style.RESET_ALL)
        sys.exit(1)
    
    print(Fore.GREEN + "✓ Connection established and validated successfully!\n" + Style.RESET_ALL)
    
    asyncio.run(interactive_console(db_client=db_client, dbname=dbname, user=user))

except KeyboardInterrupt:
    print(Fore.CYAN + "\n\nInterrupted by user. Exiting..." + Style.RESET_ALL)
    sys.exit(0)

except Exception as e:
    print(Fore.RED + f"\n[CRITICAL ERROR] {e}" + Style.RESET_ALL)
    import traceback
    traceback.print_exc()
    sys.exit(1)

finally:
    if db_client is not None and db_client.is_connected:
        try:
            db_client.disconnect()
            print(Fore.GREEN + "✓ Database connection closed successfully." + Style.RESET_ALL)
        except Exception as e:
            print(Fore.RED + f"Error closing connection: {e}" + Style.RESET_ALL)