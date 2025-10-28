import argparse
from pathlib import Path
from colorama import Fore, Style
from prompt_toolkit import HTML, PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import Completer, Completion

from postgres_client import PostgresClient

def help_message():
    print(Fore.YELLOW + "⚠ Path should not contain spaces!" + Style.RESET_ALL)
    print(Fore.CYAN + Style.BRIGHT + "\n=== Database Backup Utility ===" + Style.RESET_ALL)
    print(Fore.GREEN + "Available commands:" + Style.RESET_ALL)
    print()
    print(Fore.MAGENTA + "1) Full database backup:" + Style.RESET_ALL)
    print("   full database -path <destination_path> -compress <true|false>")
    print("   Example: full database -path /backups/mydb -compress true")
    print()
    print(Fore.MAGENTA + "2) Partial table backup:" + Style.RESET_ALL)
    print("   full tables -tablename <t1> -tablename <t2> -path <destination_path> -compress <true|false>")
    print("   Example: full tables -tablename users -tablename orders -path /backups/tables -compress false")
    print()
    print(Fore.MAGENTA + "3) Differential backup:" + Style.RESET_ALL)
    print("   differential backup")
    print("   Note: Requires a previous full backup. Will prompt for basis (created_at or updated_at)")
    print()
    print(Fore.MAGENTA + "4) Execute SQL:" + Style.RESET_ALL)
    print("   SQL <your_sql_query>")
    print("   Example: SQL SELECT * FROM users WHERE id < 100")
    print()
    print(Fore.MAGENTA + "5) SQL + export to CSV:" + Style.RESET_ALL)
    print("   SQL <your_sql_query> -extract -path <destination_path>")
    print("   Example: SQL SELECT * FROM users -extract -path /exports")
    print()
    print(Fore.MAGENTA + "6) Exit:" + Style.RESET_ALL)
    print("   exit | quit")
    print()

def print_sql_preview(rows: list, limit: int = 10):
    if not rows:
        print(Fore.YELLOW + "No rows returned" + Style.RESET_ALL)
        return
    for i, row in enumerate(rows):
        if i < limit:
            print(row)
        elif i == limit:
            print(f"... {len(rows) - limit} more rows hidden")
            break

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
    parser = argparse.ArgumentParser(add_help=False, exit_on_error=False)
    parser.add_argument("-path", type=Path, default=None, help="Destination path")
    parser.add_argument("-compress", type=str_to_bool_caster, default=False, help="Compression flag")
    parser.add_argument("-tablename", action='append', help="Table name (repeatable)")
    parser.add_argument("-extract", action='store_true', help="Extract SQL result to CSV")
    try:
        known_args, command_tokens = parser.parse_known_args(query.split())
        return known_args, command_tokens
    except (SystemExit, argparse.ArgumentError) as e:
        print(Fore.YELLOW + f"[PARSING ERROR] {e}" + Style.RESET_ALL)
        return None, None
    except Exception as e:
        print(f"Unexpected parsing error: {e}")
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
                if cmd.lower().startswith(text_before_cursor.lower()):
                    yield Completion(cmd, start_position=-len(text_before_cursor))

async def interactive_console(db_client: PostgresClient, dbname: str, user: str):
    history_file = Path.home() / ".db_backup_history"
    session = PromptSession(
        history=FileHistory(str(history_file)),
        auto_suggest=AutoSuggestFromHistory(),
        completer=SQLCompleter(),
        complete_while_typing=True,
        enable_history_search=True,
    )

    print(f"{'='*80}")
    print(Fore.CYAN + "Database Backup Utility" + Style.RESET_ALL)
    print(f"Connected to: {Fore.GREEN}{dbname}{Style.RESET_ALL} as {Fore.GREEN}{user}{Style.RESET_ALL}")
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
            path = parsed_args.path
            compress = parsed_args.compress
            tables = parsed_args.tablename or []
            has_extract = parsed_args.extract

            if command in ['exit', 'quit']:
                print(Fore.CYAN + "Goodbye! 👋" + Style.RESET_ALL)
                break

            if command == "help":
                help_message()
                continue

            if command == "full database":
                if not path:
                    print(Fore.YELLOW + "[ERROR] Path is required. Use: full database -path <path>" + Style.RESET_ALL)
                    continue
                db_client.backup_full(outpath=path, export_type="csv", compress=compress)
                continue

            if command == "full tables":
                if not path:
                    print(Fore.YELLOW + "[ERROR] Path is required. Use: full tables -path <path>" + Style.RESET_ALL)
                    continue
                if not tables:
                    print(Fore.YELLOW + "[ERROR] Provide at least one -tablename <name>" + Style.RESET_ALL)
                    continue
                db_client.partial_backup(tables=tables, outpath=path, compress=compress)
                continue
        
            if command == "differential backup":
                
                check_last_full = db_client.get_tables()

                if not check_last_full:
                    print(Fore.YELLOW + f"[ERROR] No full backup found. Differential backup cannot proceed." + Style.RESET_ALL)
                    continue

                # Ask the user for the differential backup basis
                basis = await session.prompt_async(
                    "Perform differential backup based on 'created_at' or 'updated_at'? "
                )
                basis = basis.strip().lower()

                if basis not in ["updated_at", "created_at"]:
                    print(Fore.YELLOW + "[ERROR] Invalid input. Please choose 'created_at' or 'updated_at'." + Style.RESET_ALL)
                    continue

                db_client.perform_differential_backup(basis=basis, tables=check_last_full)
                continue
                
            if command_tokens and command_tokens[0].lower() == "sql":
                sql_query_text = " ".join(command_tokens[1:])
                if not sql_query_text:
                    print(Fore.YELLOW + "[ERROR] No SQL query provided. Use: SQL <query>" + Style.RESET_ALL)
                    continue
                if not has_extract:
                    result = db_client.execute_query(sql_query_text)
                    if result is None:
                        continue
                    rows, columns = result
                    if columns:
                        print_sql_preview(rows)
                else:
                    if not path:
                        print(Fore.YELLOW + "[ERROR] Path required. Use: SQL <query> -extract -path <path>" + Style.RESET_ALL)
                        continue
                    db_client.extract_sql_query(sql_query_text, path)
                continue

            if command:
                print(Fore.YELLOW + f"Unknown command: '{command}'" + Style.RESET_ALL)
                print("Type 'help' for available commands")

        except KeyboardInterrupt:
            print()
            continue
        except EOFError:
            print(Fore.CYAN + "\nGoodbye! 👋" + Style.RESET_ALL)
            break
        except Exception as e:
            print(Fore.RED + f"Unexpected error: {e}" + Style.RESET_ALL)
            import traceback
            traceback.print_exc()