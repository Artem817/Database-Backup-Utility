import argparse
from pathlib import Path
from prompt_toolkit import HTML, PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completer, Completion
from typing import Union
from prompt_toolkit.shortcuts import radiolist_dialog
from prompt_toolkit.styles import Style
from commands.registry import build_dispatcher 

from clients.postgres_client import PostgresClient
from clients.mysql_client import MysqlClient
from console_utils import get_messenger, MessageLevel
from enum import Enum

class StorageType(Enum):
    LOCAL = "local"
    S3 = "s3"
    # GCS = "gcs" 
    # AZURE = "azure"

def print_sql_preview(rows: list, limit: int = 10):
    messenger = get_messenger()
    if not rows:
        messenger.warning("No rows returned")
        return
    for i, row in enumerate(rows):
        if i < limit:
            print(row)
        elif i == limit:
            print(f"... {len(rows) - limit} more rows hidden")
            break

async def select_storage_type() -> StorageType:
    """Selecting a storage location without a dialogue box, directly in the console."""
    
    messenger = get_messenger()
    session = PromptSession()
    
    options = [
        (1, StorageType.LOCAL, "Local Filesystem"),
        (2, StorageType.S3, "AWS S3 Bucket Not Implemented"),
        #(3, StorageType.GCS, "Google Cloud Storage Not Implemented"),
    ]
    
    print("\n--- Storage Configuration ---")
    for num, enum_val, desc in options:
        print(f"  {messenger._get_colored_message(str(num), MessageLevel.INFO)}. {desc}")
    print("-----------------------------\n")

    while True:
        try:
            selection = await session.prompt_async(HTML('<b>Select storage [1/2]:</b> '))
            
            if not selection:
                return StorageType.LOCAL

            selection = selection.strip()
            #TODO: Implement
            if selection == "2":
                messenger.warning("AWS S3 storage is not yet implemented. Defaulting to Local Filesystem.")
                return StorageType.LOCAL
            
            try:
                choice_index = int(selection)
                if 1 <= choice_index <= len(options):
                    return options[choice_index - 1][1]
                else:
                    messenger.warning("Invalid choice. Please enter 1 or 2.")
            except ValueError:
                messenger.warning("Invalid input. Please enter a number (1 or 2).")
                
        except KeyboardInterrupt:
            return StorageType.LOCAL
        
def str_to_bool_caster(v):
    if isinstance(v, bool):
        return v
    lv = v.lower()
    if lv in ('yes', 'true', 't', 'y', '1'):
        return True
    if lv in ('no', 'false', 'f', 'n', '0'):
        return False
    raise argparse.ArgumentTypeError('Boolean value expected.')

def parse_query_args(query: str):
    messenger = get_messenger()
    parser = argparse.ArgumentParser(add_help=False, exit_on_error=False)
    parser.add_argument("-path", type=Path, default=None, help="Destination path")
    parser.add_argument("-compress", type=str_to_bool_caster, default=False, help="Compression flag")
    parser.add_argument("-tablename", action='append', help="Table name (repeatable)")
    parser.add_argument("-extract", action='store_true', help="Extract SQL result to CSV")
    parser.add_argument("-single-archive", type=str_to_bool_caster, default=True, help="Create single .tar.zst archive")
    try:
        known_args, command_tokens = parser.parse_known_args(query.split())
        return known_args, command_tokens
    except (SystemExit, argparse.ArgumentError) as e:
        messenger.warning(f"[PARSING ERROR] {e}")
        return None, None
    except Exception as e:
        messenger.error(f"Unexpected parsing error: {e}")
        return None, None

class SQLCompleter(Completer):
    keywords = [
        'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE',
        'JOIN', 'LEFT JOIN', 'RIGHT JOIN', 'INNER JOIN', 'OUTER JOIN',
        'ORDER BY', 'GROUP BY', 'HAVING', 'LIMIT', 'OFFSET',
        'AS', 'AND', 'OR', 'NOT', 'IN', 'LIKE', 'BETWEEN',
        'COUNT', 'SUM', 'AVG', 'MAX', 'MIN', 'DISTINCT',
    ]
    commands = ['help', 'exit', 'quit', 'full database', 'full tables', 'differential backup', 'SQL', '-path', '-tablename', '-extract']

    def get_completions(self, document, complete_event):
        word_before_cursor = document.get_word_before_cursor()
        text_before_cursor = document.text_before_cursor
        if text_before_cursor.upper().startswith('SQL'):
            for keyword in self.keywords:
                if keyword.startswith(word_before_cursor.upper()):
                    yield Completion(keyword, start_position=-len(word_before_cursor))
        else:
            for cmd in self.commands:
                if cmd.startswith(word_before_cursor.lower()):
                    yield Completion(cmd, start_position=-len(word_before_cursor))



async def interactive_console(db_client: Union[PostgresClient, MysqlClient], dbname: str, user: str):
    
    messenger = get_messenger()
    
    print(f"{'='*80}")
    messenger.info("Database Backup Utility - Initial Configuration")
    print()
    
    storage_type = await select_storage_type()
    
    messenger.success(f"✓ Storage configured: {storage_type.value}")
    
    db_client._storage_type = storage_type.value
    
    dispatcher = build_dispatcher(db_client, messenger, storage_type= storage_type.value)
    
    history_file = Path.home() / ".db_backup_history"
    session = PromptSession(
        history=FileHistory(str(history_file)),
        auto_suggest=AutoSuggestFromHistory(),
        completer=SQLCompleter(),
        complete_while_typing=True,
        enable_history_search=True,
    )

    print(f"\n{'='*80}")

    messenger.section_header(f"Storage: {storage_type.value.upper()}")
    messenger.info("")

    messenger.info("Database Backup Utility")
    print(f"Connected to: {messenger._get_colored_message(dbname, MessageLevel.SUCCESS)} as {messenger._get_colored_message(user, MessageLevel.SUCCESS)}")
    

    #print(f"Storage: {messenger._get_colored_message(storage_type.value.upper(), MessageLevel.INFO)}")
    print("Type 'help' for commands or 'exit' to quit\n")

    while True:
        try:
            query = await session.prompt_async(HTML(f'<ansigreen>[{dbname}]&gt;</ansigreen> '))
            query = query.strip()
            if not query:
                continue

            parsed_args, command_tokens = parse_query_args(query)
            if parsed_args is None:
                continue

            command = " ".join(command_tokens).lower()

            if command in ['exit', 'quit']:
                messenger.info("Goodbye! 👋")
                break
            try:
                dispatcher.dispatch(command, parsed_args)
            except ValueError as e:
                messenger.error(str(e))
            except Exception as e:
                messenger.error(f"Command execution failed: {e}")
                import traceback
                traceback.print_exc()
            
        except KeyboardInterrupt:
            print()
            continue
        except EOFError:
            messenger.info("\nGoodbye! 👋")
            break
        except Exception as e:
            messenger.error(f"Unexpected error: {e}")
            import traceback
            traceback.print_exc()