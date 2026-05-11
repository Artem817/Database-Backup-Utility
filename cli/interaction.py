import argparse
import re
import shlex
from dataclasses import dataclass
from html import escape
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
from services.backup.catalog import BackupCatalog
from utility.wal_segments import SEGMENTS_PER_LOG

WAL_FILENAME_PATTERN = re.compile(r"^[0-9A-F]{24}$")


@dataclass(frozen=True)
class PromptState:
    database_name: str
    current_wal_file: str | None
    new_wal_files: int | None

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


def short_wal(wal_file: str | None) -> str:
    """Return the short WAL suffix used in the prompt."""
    if not wal_file:
        return "?"

    normalized = wal_file.strip().upper()
    if not normalized:
        return "?"
    return normalized[-2:]


def get_current_state(
    db_client: Union[PostgresClient, MysqlClient],
    dbname: str,
) -> PromptState:
    """
    Fetch fresh prompt state.

    This intentionally does not cache values between calls. Keep this path cheap:
    one PostgreSQL metadata query plus one catalog lookup.
    """
    try:
        current_wal_file = _fetch_current_wal_file(db_client)
        last_backup_wal_file = _fetch_last_backup_wal_file(dbname)
        new_wal_files = _count_wal_segments_between(
            last_backup_wal_file,
            current_wal_file,
        )

        return PromptState(
            database_name=dbname,
            current_wal_file=current_wal_file,
            new_wal_files=new_wal_files,
        )
    except Exception:
        _rollback_if_possible(db_client)
        return PromptState(
            database_name=dbname,
            current_wal_file=None,
            new_wal_files=None,
        )


def _fetch_current_wal_file(db_client: Union[PostgresClient, MysqlClient]) -> str | None:
    if not isinstance(db_client, PostgresClient):
        return None
    if not db_client.is_connected or db_client.connection is None:
        return None

    with db_client.connection.cursor() as cursor:
        cursor.execute("SELECT pg_walfile_name(pg_current_wal_lsn());")
        row = cursor.fetchone()

    if not row or not row[0]:
        return None
    return str(row[0]).strip().upper()


def _fetch_last_backup_wal_file(dbname: str) -> str | None:
    last_backup = BackupCatalog().get_last_successful_backup(database_name=dbname)
    if not last_backup:
        return None

    for key in ("current_wal_file", "last_backup_wal_file", "previous_wal_file"):
        wal_file = last_backup.get(key)
        if isinstance(wal_file, str) and wal_file.strip():
            return wal_file.strip().upper()

    lsn_markers = last_backup.get("lsn_markers")
    if isinstance(lsn_markers, dict):
        for key in ("current_wal_file", "last_backup_wal_file", "previous_wal_file"):
            wal_file = lsn_markers.get(key)
            if isinstance(wal_file, str) and wal_file.strip():
                return wal_file.strip().upper()

    return None


def _count_wal_segments_between(
    last_backup_wal_file: str | None,
    current_wal_file: str | None,
) -> int | None:
    last_position = _wal_position(last_backup_wal_file)
    current_position = _wal_position(current_wal_file)

    if last_position is None or current_position is None:
        return None

    last_timeline, last_segment_number = last_position
    current_timeline, current_segment_number = current_position
    if last_timeline != current_timeline or current_segment_number < last_segment_number:
        return None

    return current_segment_number - last_segment_number


def _wal_position(wal_file: str | None) -> tuple[int, int] | None:
    if not wal_file:
        return None

    normalized = wal_file.strip().upper()
    if not WAL_FILENAME_PATTERN.match(normalized):
        return None

    timeline = int(normalized[0:8], 16)
    log = int(normalized[8:16], 16)
    segment = int(normalized[16:24], 16)
    return timeline, (log * SEGMENTS_PER_LOG) + segment


def _rollback_if_possible(db_client: Union[PostgresClient, MysqlClient]) -> None:
    rollback = getattr(db_client, "rollback", None)
    if callable(rollback):
        try:
            rollback()
        except Exception:
            pass


def _format_prompt(state: PromptState) -> HTML:
    wal_suffix = escape(short_wal(state.current_wal_file))
    wal_count = "?" if state.new_wal_files is None else str(state.new_wal_files)
    database_name = escape(state.database_name)
    return HTML(
        f"<ansigreen>[{database_name} | WAL:{wal_suffix} | +{wal_count}]&gt;</ansigreen> "
    )

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
    parser.add_argument("-path", type=Path, default=None, help="Backup input/output path")
    parser.add_argument("-backup-path", type=Path, default=None, help="Path to full backup directory or .tar.zst archive")
    parser.add_argument("-data-dir", type=Path, default=None, help="New PostgreSQL data directory")
    parser.add_argument("-wal-path", type=Path, default=None, help="Path to PostgreSQL WAL archive directory")
    parser.add_argument("-archive-dir", type=Path, default=None, help="Alias for -wal-path")
    parser.add_argument("-target-time", default=None, help="PostgreSQL PITR target time; values without timezone are UTC")
    parser.add_argument("-catalog-path", default="backup_catalog.db", help="Backup catalog path for PITR base backup selection")
    parser.add_argument("-restore-port", type=int, default=None, help="Port for restored PostgreSQL")
    parser.add_argument("-port", type=int, default=None, help="Port for restored MySQL")
    parser.add_argument("-verify-user", default=None, help="User for restore verification query")
    parser.add_argument("-verify-query", default="SELECT 1;", help="SQL query for restore verification")
    parser.add_argument("-pg-bin-dir", type=Path, default=None, help="Directory with pg_ctl and psql binaries")
    parser.add_argument("-mysql-bin-dir", type=Path, default=None, help="Directory with xtrabackup, mysqld, and mysqladmin binaries")
    parser.add_argument("-mysql-socket", default="/tmp/mysql_restore.sock", help="Socket path for restored MySQL")
    parser.add_argument("-mysql-owner", default=None, help="Owner user for restored MySQL data directory")
    parser.add_argument("-mysql-group", default=None, help="Owner group for restored MySQL data directory")
    parser.add_argument("-validate-restore", type=str_to_bool_caster, default=True, help="Run automated test-restore after PostgreSQL full backup")
    parser.add_argument("-restore-validation-port", type=int, default=0, help="Port for automated restore validation; 0 selects a free port")
    parser.add_argument("-restore-validation-keep-data", type=str_to_bool_caster, default=False, help="Keep temporary restore data after validation")
    parser.add_argument("-restore-validation-checksums", type=str_to_bool_caster, default=True, help="Compare per-table restore checksums")
    parser.add_argument("-compress", type=str_to_bool_caster, default=False, help="Compression flag")
    parser.add_argument("-extract", action='store_true', help="Extract SQL result to CSV")
    parser.add_argument("-single-archive", type=str_to_bool_caster, default=True, help="Create single .tar.zst archive")
    try:
        known_args, command_tokens = parser.parse_known_args(shlex.split(query))
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
    commands = [
        'help',
        'exit',
        'quit',
        'full database',
        'differential backup',
        'restore',
        'restore database',
        'SQL',
        '-path',
        '-backup-path',
        '-data-dir',
        '-wal-path',
        '-archive-dir',
        '-target-time',
        '-catalog-path',
        '-restore-port',
        '-port',
        '-verify-user',
        '-verify-query',
        '-pg-bin-dir',
        '-mysql-bin-dir',
        '-mysql-socket',
        '-mysql-owner',
        '-mysql-group',
        '-validate-restore',
        '-restore-validation-port',
        '-restore-validation-keep-data',
        '-restore-validation-checksums',
        '-extract',
        '-compress',
        '-single-archive',
    ]

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



async def interactive_console(
    db_client: Union[PostgresClient, MysqlClient],
    dbname: str,
    user: str,
    initial_storage_type: str | None = None,
):
    
    messenger = get_messenger()
    
    print(f"{'='*80}")
    messenger.info("Database Backup Utility - Initial Configuration")
    print()
    
    if initial_storage_type == StorageType.LOCAL.value:
        storage_type = StorageType.LOCAL
    elif initial_storage_type == "cloud":
        messenger.warning(
            "Cloud storage is not yet implemented. Defaulting to Local Filesystem."
        )
        storage_type = StorageType.LOCAL
    else:
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
            prompt_state = get_current_state(db_client, dbname)
            query = await session.prompt_async(_format_prompt(prompt_state))
            query = query.strip()
            if not query:
                continue

            parsed_args, command_tokens = parse_query_args(query)
            if parsed_args is None:
                continue

            command = " ".join(command_tokens)

            if command.lower() in ['exit', 'quit']:
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
