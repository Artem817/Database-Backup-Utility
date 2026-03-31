from .command_dispatcher import CommandDispatcher
from services.backup_services import BackupService
from console_utils import MessageLevel

def build_dispatcher(db_client, messenger,  storage_type: str = "local"):
    dispatcher = CommandDispatcher(storage_type = storage_type)
    backup_service = BackupService(db_client)
    
    def help_command(parsed_args):
        messenger.section_header("Database Backup Utility")
        messenger.success("Available commands:")
        print()
        messenger.print_colored("1) Full backup:", MessageLevel.INFO)
        print("   full database -path <destination_path>")
        print("   Example: full database -path /backups/postgres")
        print("   Note: Creates a physical full backup using the utility for the selected database")
        print()
        messenger.print_colored("2) Differential backup:", MessageLevel.INFO)
        print("   differential backup")
        print("   Example: differential backup")
        print("   Note: PostgreSQL copies archived WAL files; MySQL runs xtrabackup incremental")
        print()
        messenger.print_colored("3) Execute SQL:", MessageLevel.INFO)
        print("   SQL <your_sql_query>")
        print("   Example: SQL SELECT * FROM users WHERE id < 100")
        print()
        messenger.print_colored("4) SQL + export to CSV:", MessageLevel.INFO)
        print("   SQL <your_sql_query> -extract -path <destination_path>")
        print("   Example: SQL SELECT * FROM users -extract -path /exports")
        print()
        messenger.print_colored("5) Exit:", MessageLevel.INFO)
        print("   exit | quit")
        print()
    
    dispatcher.register_command("full_backup", backup_service.full_backup)
    dispatcher.register_command("differential_backup", backup_service.differential_backup)
    dispatcher.register_command("execute_sql", backup_service.execute_sql)
    dispatcher.register_command("help", help_command)
    
    return dispatcher
