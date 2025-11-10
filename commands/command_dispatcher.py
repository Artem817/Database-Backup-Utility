class CommandDispatcher:
    def __init__(self, storage_type: str = "local"):
        self.commands = {}
        self.storage_type = storage_type  
        
    def register_command(self, command_name: str, handler):
        self.commands[command_name.lower()] = handler
        
    def dispatch(self, command_name: str, parsed_args):
        """Dispatch command with parsed arguments"""
        command_name = command_name.lower()
        
        command_mapping = {
            "full database": "full_backup",
            "full tables": "partial_backup", 
            "differential backup": "differential_backup",
            "help": "help"
        }
        
        if command_name.startswith("sql "):
            mapped_command = "execute_sql"
            sql_query = command_name[4:].strip() 
            return self.execute_command(mapped_command, sql_query, parsed_args)
        
        mapped_command = command_mapping.get(command_name, command_name)
        
        if mapped_command not in self.commands:
            raise ValueError(f"Command '{command_name}' not recognized.")
        
        if mapped_command in ["full_backup", "partial_backup", "differential_backup"]:
            parsed_args.storage_type = self.storage_type
            
        return self.execute_command(mapped_command, parsed_args)
        
    def execute_command(self, command_name: str, *args, **kwargs):
        command_name = command_name.lower()
        if command_name not in self.commands:
            raise ValueError(f"Command '{command_name}' not recognized.")
        return self.commands[command_name](*args, **kwargs)