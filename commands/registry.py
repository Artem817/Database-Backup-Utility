from .command_dispatcher import CommandDispatcher
from services.backup_services import BackupService
from console_utils import MessageLevel

def build_dispatcher(db_client, messenger):
    dispatcher = CommandDispatcher()
    backup_service = BackupService(db_client)
    
    def help_command(parsed_args):
        messenger.warning("Path should not contain spaces!")
        messenger.section_header("Database Backup Utility")
        messenger.success("Available commands:")
        print()
        messenger.print_colored("1) Full database backup:", MessageLevel.INFO)
        print("   full database -path <destination_path>")
        print("   Example: full database -path /backups/mydb")
        print("   Note: Automatically compressed with zstd")
        print()
        messenger.print_colored("2) Partial table backup:", MessageLevel.INFO)
        print("   full tables -tablename <t1> -tablename <t2> -path <destination_path>")
        print("   Example: full tables -tablename users -tablename orders -path /backups/tables")
        print("   Note: Automatically compressed with zstd")
        print()
        messenger.print_colored("3) Differential backup:", MessageLevel.INFO)
        print("   differential backup")
        print("   Example: differential backup")
        print("   Note: Requires previous full backup, exports only changed rows since last full backup")
        print()
        messenger.print_colored("4) Execute SQL:", MessageLevel.INFO)
        print("   SQL <your_sql_query>")
        print("   Example: SQL SELECT * FROM users WHERE id < 100")
        print()
        messenger.print_colored("5) SQL + export to CSV:", MessageLevel.INFO)
        print("   SQL <your_sql_query> -extract -path <destination_path>")
        print("   Example: SQL SELECT * FROM users -extract -path /exports")
        print()
        messenger.print_colored("6) Exit:", MessageLevel.INFO)
        print("   exit | quit")
        print()
    
    dispatcher.register_command("full_backup", backup_service.full_backup)
    dispatcher.register_command("partial_backup", backup_service.partial_backup)
    dispatcher.register_command("differential_backup", backup_service.differential_backup)
    dispatcher.register_command("execute_sql", backup_service.execute_sql)
    dispatcher.register_command("help", help_command)
    
    return dispatcher
