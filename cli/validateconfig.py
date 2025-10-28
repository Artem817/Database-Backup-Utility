import os
import sys
from console_utils import get_messenger


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
    else:
        parser.error(f"Unsupported config type: {args.config}")