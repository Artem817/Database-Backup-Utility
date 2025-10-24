import psycopg2
from colorama import Fore, Style

def validate_pg_connection(dbname, user, host, password, port):
    """
    Validates and establishes PostgreSQL connection
    
    Args:
        dbname: Database name
        user: Username
        host: Host address
        password: Password
        port: Port number
        
    Returns:
        psycopg2.connection object or None if connection fails
    """
    try:
        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            host=host,
            password=password,
            port=port,
            connect_timeout=10  
        )
        
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            version = cur.fetchone()[0]
            print(Fore.GREEN + f"✓ PostgreSQL connection successful!" + Style.RESET_ALL)
            print(Fore.CYAN + f"  Server version: {version.split(',')[0]}" + Style.RESET_ALL)
        
        return conn
    except psycopg2.OperationalError as e:
        print(f"Error: Unable to connect to PostgreSQL database. Details: {e}\n")
        print("Check that the data in .env is correct.")
        return None
