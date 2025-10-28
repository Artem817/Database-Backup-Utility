import csv
from pathlib import Path
from typing import Dict
from colorama import Fore, Style
import psycopg2
import sqlparse
from psycopg2.extensions import connection
from datetime import datetime, timezone
import os
import subprocess
import json
import shutil
import sys
import oschmod


from factory import DatabaseClient
from custom_logging import BackupLogger, BackupCatalog

def analyze_sql(query: str) -> tuple[bool, str]:
    """Analyze SQL for destructive operations."""
    if not query or not query.strip():
        return True, "Empty query."
    dangerous = {"DROP", "DELETE", "TRUNCATE", "ALTER"}
    try:
        parsed = sqlparse.parse(query)
        if not parsed:
            return True, "Empty query."
        tokens = [t.value.upper() for t in parsed[0].tokens if not t.is_whitespace]
        found = [w for w in dangerous if w in tokens]
        if found:
            return False, f"The query contains dangerous keywords: {', '.join(found)}"
        return True, "Looks safe."
    except Exception as e:
        return False, f"SQL analysis failed: {e}"

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

class PostgresClient(DatabaseClient):
    def __init__(self, host, database, user, password, port=5432, utility_version="1.0.0"):
        self._host = host
        self._database = database
        self._user = user
        self._password = password
        self._port = port
        self._connection: connection = None
        self._utility_version = utility_version

        self._logger = BackupLogger(name=f"backup_{database}", log_file=f"backup_{database}.log")
        self._database_version = None
        self.compress: bool = False

    @property
    def connection(self):
        return self._connection

    @property
    def database_name(self):
        return self._database

    @property
    def connection_params(self):
        return {"host": self._host, "user": self._user, "database": self._database, "port": self._port}

    @property
    def is_connected(self):
        return self._connection is not None and not self._connection.closed

    def connect(self):
        try:
            self._connection = psycopg2.connect(
                dbname=self._database, user=self._user, host=self._host,
                password=self._password, port=self._port, connect_timeout=10
            )
            with self._connection.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                self._database_version = version.split(',')[0]
                print(Fore.GREEN + "✓ PostgreSQL connection successful!" + Style.RESET_ALL)
                print(Fore.CYAN + f"  Server version: {self._database_version}" + Style.RESET_ALL)
                self._logger.info(f"Connected to database: {self._database} ({self._database_version})")
            return self._connection
        except psycopg2.OperationalError as e:
            print(Fore.RED + f"Error: Unable to connect. Details: {e}" + Style.RESET_ALL)
            print(Fore.YELLOW + "Check your .env/CLI settings." + Style.RESET_ALL)
            self._logger.error(f"Connection failed: {e}")
            return None

    def disconnect(self):
        try:
            if self._connection and not self._connection.closed:
                self._connection.close()
                self._connection = None
                print(Fore.YELLOW + "Disconnected from database." + Style.RESET_ALL)
                self._logger.info("Database connection closed")
        except psycopg2.OperationalError as e:
            print(Fore.RED + f"Error on disconnect: {e}" + Style.RESET_ALL)
            self._logger.error(f"Disconnect failed: {e}")
            return None

    def _execute(self, query):
        cur = self._connection.cursor()
        cur.execute(query)
        return cur

    def fetch_all(self, query):
        cur = self._execute(query)
        return cur.fetchall()

    def fetch_one(self, query):
        cur = self._execute(query)
        return cur.fetchone()

    def commit(self):
        return self._connection.commit()

    def rollback(self):
        return self._connection.rollback()

    def validate_connection(self):
        try:
            if not self._connection or self._connection.closed:
                return False
            with self._connection.cursor() as cur:
                cur.execute("SELECT 1;")
                return cur.fetchone()[0] == 1
        except Exception:
            return False

    def get_tables(self):
        """Return list of (schema, table_name) for user tables."""
        with self.connection.cursor() as cur:
            cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema');
            """)
            return cur.fetchall()

    def get_table_schema(self, table_name: str, schema: str = "public"):
        try:
            with self.connection.cursor() as cur:
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s;
                """, (schema, table_name))
                return cur.fetchall()
        except Exception as e:
            print(Fore.RED + f"Failed to get schema for {table_name}: {e}" + Style.RESET_ALL)
            self._logger.error(f"Schema retrieval failed for {table_name}: {e}")
            return []

    def get_database_size(self) -> str:
        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT pg_size_pretty(pg_database_size(%s));", (self._database,))
                size = cur.fetchone()[0]
                print(Fore.GREEN + f"Database size: {size}" + Style.RESET_ALL)
                return size
        except Exception as e:
            print(Fore.RED + f"Failed to get DB size: {e}" + Style.RESET_ALL)
            return "Unknown"

    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        with self.connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                );
            """, (schema, table_name))
            return cur.fetchone()[0]
    

    def _create_backup_structure(self, base_path: Path, backup_id: str, back_up_time) -> dict:
        backup_root = base_path / backup_id
        data_dir = backup_root / "data"
        backup_diff_dir = backup_root / ".backup_diff"
        
        data_dir.mkdir(parents=True, exist_ok=True)
        backup_diff_dir.mkdir(parents=True, exist_ok=True)
        
        oschmod.set_mode(backup_diff_dir, "700")
        
        manifest_path = backup_diff_dir / "manifest.json"
        manifest_data = {
            "base_backup": back_up_time,
            "diff_chain": [],
            "last_diff_timestamp": back_up_time
        }
        with open(manifest_path, "w", encoding="utf-8") as manifest_file:
            json.dump(manifest_data, manifest_file, indent=4, ensure_ascii=False)
        
        oschmod.set_mode(manifest_path, "600") 
        
        return {
            "root": backup_root,
            "data": data_dir,
            "schema": backup_root / "schema.sql",
            "metadata": backup_root / "metadata.json",
            "diff_root": backup_diff_dir,
            "manifest": manifest_path
        }

    
    def export_table(self, tables, outpath, metadata=None) -> list[dict]:
        saved_files = []
        outpath = Path(outpath) if isinstance(outpath, str) else outpath
        if not self._prepare_output_directory(outpath):
            return []

        for schema, table_name in tables:
            file_path = outpath / f"{table_name}.csv"
            export_result = self._export_single_table(schema, table_name, file_path, metadata)
            if export_result:
                saved_files.append(export_result)

        return saved_files

    def _prepare_output_directory(self, outpath: Path) -> bool:
        try:
            outpath.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            print(Fore.RED + f"Failed to create {outpath}: {e}" + Style.RESET_ALL)
            self._logger.error(f"Dir creation failed: {e}")
            return False

    def _export_single_table(self, schema: str, table_name: str, file_path: Path, metadata=None, where: str = None) -> dict | None:
        full_table_name = f'"{schema}"."{table_name}"'
        try:
            query = f"SELECT * FROM {full_table_name}"
            if where:
                query += f" WHERE {where}"
            with self.connection.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                if len(rows) == 0 and where:
                    selected_columns = self._export_single_table(schema, full_table_name, file_path, metadata)
                    if selected_columns is not None:
                        return selected_columns
                columns = [d[0] for d in cur.description]
                self._write_table_to_csv(file_path, columns, rows)
                file_size = file_path.stat().st_size
                self._log_table_backup(metadata, table_name, len(rows), file_size, str(file_path))
                print(Fore.GREEN + f"✓ Saved: {file_path.name} ({len(rows)} rows, {file_size / 1024:.2f} KB)" + Style.RESET_ALL)
                return {
                    "table_name": table_name,
                    "file_path": str(file_path),
                    "rows_count": len(rows),
                    "file_size": file_size
                }
                
        except Exception as e:
            print(Fore.RED + f"✗ Export {table_name} failed: {e}" + Style.RESET_ALL)
            self._logger.error(f"Table export failed for {table_name}: {e}")
            return None

    def _write_table_to_csv(self, file_path: Path, columns: list, rows: list):
        try:
            with file_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
        except Exception as e:
            print(Fore.RED + f"Failed to write CSV: {e}" + Style.RESET_ALL)
            self._logger.error(f"CSV write failed: {e}")

    def _log_table_backup(self, metadata: dict, table_name: str, rows_count: int, file_size: int, file_path: str):
        if metadata:
            self._logger.log_table_backup(
                metadata=metadata,
                table_name=table_name,
                rows_count=rows_count,
                file_size=file_size,
                file_path=file_path
            )

    def database_schema(self, outpath):
        try:
            outpath = Path(outpath) if isinstance(outpath, str) else outpath
            if not outpath.exists():
                outpath.parent.mkdir(parents=True, exist_ok=True)
            command = [
                "pg_dump",
                "--host", self._host,
                "--port", str(self._port),
                "--username", self._user,
                "--dbname", self._database,
                "--schema-only",
            ]
            env = os.environ.copy()
            env["PGPASSWORD"] = self._password
            with open(outpath, "w", encoding="utf-8") as f:
                subprocess.run(command, stdout=f, check=True, env=env)
            print(Fore.GREEN + f"✓ Schema exported: {outpath}" + Style.RESET_ALL)
            return str(outpath)
        except subprocess.CalledProcessError as e:
            print(Fore.RED + f"pg_dump failed: {e}" + Style.RESET_ALL)
            self._logger.error(f"pg_dump failed: {e}")
            return None
        except Exception as e:
            print(Fore.RED + f"Schema export error: {e}" + Style.RESET_ALL)
            self._logger.error(f"Schema export failed: {e}")
            return None

    def _save_metadata(self, metadata: dict, filepath: Path):
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=4, ensure_ascii=False)
            print(Fore.GREEN + f"✓ Metadata saved: {filepath}" + Style.RESET_ALL)
        except Exception as e:
            print(Fore.RED + f"Failed to save metadata: {e}" + Style.RESET_ALL)
            self._logger.error(f"Metadata save failed: {e}")

    def backup_full(self, outpath, export_type: str = "csv", compress: bool = False):
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        print(Fore.YELLOW + f"Starting full backup → {base_path}" + Style.RESET_ALL)
        if compress:
            print(Fore.CYAN + "Compression enabled" + Style.RESET_ALL)

        metadata = self._logger.start_backup(
            backup_type="full",
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version=self._utility_version,
            compress=compress
        )

        try:
            backup_structure = self._create_backup_structure(base_path, metadata["id"], back_up_time=metadata["timestamp_start"])
            print(Fore.CYAN + f"Backup dir: {backup_structure['root']}" + Style.RESET_ALL)

            schema_path = self.database_schema(backup_structure["schema"])
            if schema_path:
                metadata["schema_file"] = str(backup_structure["schema"])

            tables = self.get_tables()
            
            if not tables:
                print(Fore.YELLOW + "No tables found" + Style.RESET_ALL)
                self._logger.warning("No tables for backup")
                self._logger.finish_backup(metadata, success=False)
                return False

            print(Fore.CYAN + f"Found {len(tables)} table(s)..." + Style.RESET_ALL)
            export = self.export_table(tables, backup_structure["data"], metadata=metadata)
            if not export:
                print(Fore.RED + "✗ Backup failed - no files exported" + Style.RESET_ALL)
                self._logger.finish_backup(metadata, success=False)
                return False

            metadata["backup_location"] = str(backup_structure["root"])
            self._logger.finish_backup(metadata, success=True)
            self._save_metadata(metadata, backup_structure["metadata"])

            if compress:
                print(Fore.CYAN + "Compressing..." + Style.RESET_ALL)
                self.compress_backup(backup_structure['root'])

            print(Fore.GREEN + "✓ Full backup completed" + Style.RESET_ALL)
            return True
        
        except Exception as e:
            print(Fore.RED + f"Backup failed: {e}" + Style.RESET_ALL)
            self._logger.error(f"Backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False

    def _column_exists(self, schema: str, table_name: str, column_name: str) -> bool:
        try:
            with self.connection.cursor() as cur:
                cur.execute("SAVEPOINT check_column")
                try:
                    cur.execute("""
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s AND column_name = %s;
                    """, (schema, table_name, column_name))
                    result = cur.fetchone() is not None
                    cur.execute("RELEASE SAVEPOINT check_column")
                    return result
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT check_column")
                    return False
        except Exception as e:
            self._logger.error(f"Column check failed: {e}")
            return False
    
    def partial_backup(self, tables: list, outpath: str, backup_type: str = "partial", compress: bool = False):
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        print(Fore.YELLOW + f"Starting {backup_type} backup → {base_path}" + Style.RESET_ALL)
        if compress:
            print(Fore.CYAN + "Compression enabled" + Style.RESET_ALL)

        metadata = self._logger.start_backup(
            backup_type=backup_type,
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version=self._utility_version,
            compress=compress
        )

        try:
            backup_structure = self._create_backup_structure(base_path, metadata["id"],back_up_time =metadata["timestamp_start"])
            print(Fore.CYAN + f"Backup dir: {backup_structure['root']}" + Style.RESET_ALL)

            schema_path = self.database_schema(backup_structure["schema"])
            if schema_path:
                metadata["schema_file"] = str(backup_structure["schema"])

            verified_tables = []
            for table in tables:
                if self.table_exists(table_name=table):
                    verified_tables.append(("public", table))
                    print(Fore.GREEN + f"✓ Table '{table}' found" + Style.RESET_ALL)
                    self._logger.info(f"Table '{table}' verified")
                else:
                    print(Fore.RED + f"✗ Table '{table}' doesn't exist" + Style.RESET_ALL)
                    self._logger.warning(f"Table '{table}' missing")

            if not verified_tables:
                print(Fore.YELLOW + "No valid tables to export" + Style.RESET_ALL)
                self._logger.finish_backup(metadata, success=False)
                return False

            export = self.export_table(verified_tables, backup_structure["data"], metadata=metadata)
            if not export:
                print(Fore.RED + "✗ Backup failed - no files exported" + Style.RESET_ALL)
                self._logger.finish_backup(metadata, success=False)
                return False

            metadata["backup_location"] = str(backup_structure["root"])
            self._logger.finish_backup(metadata, success=True)
            self._save_metadata(metadata, backup_structure["metadata"])

            if compress:
                print(Fore.CYAN + "Compressing..." + Style.RESET_ALL)
                self.compress_backup(backup_structure['root'])

            print(Fore.GREEN + "✓ Partial backup completed" + Style.RESET_ALL)
            return True
        except Exception as e:
            print(Fore.RED + f"Backup failed: {e}" + Style.RESET_ALL)
            self._logger.error(f"Partial backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False

    def execute_query(self, query: str):
        is_safe, message = analyze_sql(query)
        if not is_safe:
            print(Fore.YELLOW + message + Style.RESET_ALL)
            self._logger.warning(f"Dangerous query detected: {message}")
            if sys.stdin.isatty():
                confirmation = input("Continue? (Y/n): ")
                if confirmation.upper() != "Y":
                    self._logger.info("Query execution cancelled by user")
                    return None
            else:
                self._logger.warning("Non-interactive mode: dangerous query skipped.")
                return None

        try:
            with self.connection.cursor() as cur:
                self._logger.info(f"Executing query: {query[:100]}...")
                cur.execute(query)
                if cur.description:
                    rows = cur.fetchall()
                    columns = [d[0] for d in cur.description]
                    self._logger.info(f"Query returned {len(rows)} rows")
                    return (rows, columns)
                else:
                    self.connection.commit()
                    affected = cur.rowcount
                    print(Fore.GREEN + f"✓ Query executed. {affected} rows affected." + Style.RESET_ALL)
                    self._logger.info(f"Query executed, {affected} rows affected")
                    return ([], [])
        except Exception as e:
            print(Fore.RED + f"Query failed: {e}" + Style.RESET_ALL)
            self._logger.error(f"Query failed: {e}")
            self.connection.rollback()
            return None

    def csv_fragmental_backup(self, rows, outpath, query: str = None):
        try:
            if not rows or (isinstance(rows, tuple) and not rows[0]):
                print(Fore.YELLOW + "No data to export" + Style.RESET_ALL)
                self._logger.warning("No data to export")
                return False

            outpath = Path(outpath) if isinstance(outpath, str) else outpath
            outpath.mkdir(parents=True, exist_ok=True)

            if query:
                query_upper = query.upper().strip()
                if "FROM" in query_upper:
                    table_part = query_upper.split("FROM")[1].split()[0]
                    table_name = table_part.strip('"').strip("'").replace(".", "_")
                    filename = f"query_{table_name}_{self._database}.csv"
                else:
                    filename = f"query_result_{self._database}.csv"
            else:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"query_{timestamp}_{self._database}.csv"

            file_path = outpath / filename
            if isinstance(rows, tuple) and len(rows) == 2:
                data, columns = rows
            else:
                print(Fore.RED + "Invalid data format for CSV export" + Style.RESET_ALL)
                self._logger.error("Invalid CSV export data format")
                return False

            with file_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(data)

            file_size = file_path.stat().st_size
            print(Fore.GREEN + f"✓ Saved: {file_path} ({len(data)} rows, {file_size / 1024:.2f} KB)" + Style.RESET_ALL)
            self._logger.info(f"Query result exported: {file_path} ({len(data)} rows, {file_size} bytes)")
            return str(file_path)
        except Exception as e:
            print(Fore.RED + f"Failed to save query result: {e}" + Style.RESET_ALL)
            self._logger.error(f"CSV export failed: {e}")
            return False

    def extract_sql_query(self, query: str, outpath):
        self._logger.info(f"Starting query extraction to: {outpath}")
        execute_result = self.execute_query(query)
        if execute_result is None:
            self._logger.warning("Query extraction cancelled or failed")
            return False
        result = self.csv_fragmental_backup(execute_result, outpath, query)
        if result:
            self._logger.info(f"Query extraction completed: {result}")
        else:
            self._logger.error("Query extraction failed")
        return result

    def get_backup_history(self, limit: int = 10) -> list:
        catalog = BackupCatalog()
        backups = catalog.catalog.get("backups", [])
        sorted_backups = sorted(backups, key=lambda b: b.get("timestamp_start", ""), reverse=True)
        return sorted_backups[:limit]

    def print_backup_history(self, limit: int = 10):
        history = self.get_backup_history(limit)
        if not history:
            print(Fore.YELLOW + "No backup history found" + Style.RESET_ALL)
            return
        print(Fore.CYAN + f"\n{'='*80}" + Style.RESET_ALL)
        print(Fore.CYAN + f"Recent Backup History (last {limit})" + Style.RESET_ALL)
        print(Fore.CYAN + f"{'='*80}" + Style.RESET_ALL)
        for backup in history:
            status_color = Fore.GREEN if backup.get("status") == "completed" else Fore.RED
            print(f"\n{Fore.YELLOW}ID:{Style.RESET_ALL} {backup.get('id')}")
            print(f"{Fore.YELLOW}Type:{Style.RESET_ALL} {backup.get('type')}")
            print(f"{Fore.YELLOW}Status:{Style.RESET_ALL} {status_color}{backup.get('status')}{Style.RESET_ALL}")
            print(f"{Fore.YELLOW}Started:{Style.RESET_ALL} {backup.get('timestamp_start')}")
            print(f"{Fore.YELLOW}Duration:{Style.RESET_ALL} {backup.get('duration_seconds', 0):.2f}s")
            stats = backup.get('statistics', {})
            if stats:
                size_mb = stats.get('total_size_bytes', 0) / 1024 / 1024
                print(f"{Fore.YELLOW}Tables:{Style.RESET_ALL} {stats.get('total_tables', 0)}")
                print(f"{Fore.YELLOW}Rows:{Style.RESET_ALL} {stats.get('total_rows_processed', 0)}")
                print(f"{Fore.YELLOW}Size:{Style.RESET_ALL} {size_mb:.2f} MB")
        print(Fore.CYAN + f"\n{'='*80}\n" + Style.RESET_ALL)

    def _get_last_full_backup_info(self, info_type: str) -> str | list[str] | None:
        print(Fore.CYAN + f"Fetching last full backup info for type: {info_type}" + Style.RESET_ALL)
        catalog = BackupCatalog()
        backups = catalog.catalog.get("backups", [])
        print(Fore.CYAN + f"Total backups found: {len(backups)}" + Style.RESET_ALL)
        full_backups = [
            backup for backup in backups
            if backup.get("database_name") == self._database and backup.get("type") == "full"
        ]
        print(Fore.CYAN + f"Full backups for database '{self._database}': {len(full_backups)}" + Style.RESET_ALL)
        sorted_backups = sorted(full_backups, key=lambda b: b.get("timestamp_start", ""), reverse=True)
        if sorted_backups:
            last_backup = sorted_backups[0]
            print(Fore.CYAN + f"Last full backup found: {last_backup}" + Style.RESET_ALL)
            if info_type == "timestamp":
                ts = last_backup.get("timestamp_start")
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            elif info_type == "tables":
                tables = last_backup.get("tables", {})
                print(Fore.CYAN + f"Tables in last full backup: {list(tables.keys())}" + Style.RESET_ALL)
                return list(tables.keys())
            elif info_type == "backup_location":
                return last_backup.get("backup_location")
            
        print(Fore.YELLOW + "No full backups found." + Style.RESET_ALL)
        return None

    def get_last_full_backup_timestamp(self) -> str | None:
        return self._get_last_full_backup_info("timestamp")

    def get_table_names_from_last_full_backup(self) -> list[str]:
        return self._get_last_full_backup_info("tables") or []
    
    def get_output_path_from_last_full_backup(self) -> str | None:
        print("Retrieving last full backup location...")
        print("self._database:", self._database)
        
        return self._get_last_full_backup_info("backup_location")
    
    def get_max_updated_at(self, table_name: str, schema: str, column: str):
        try:
            query = f'SELECT MAX({column}) FROM "{schema}"."{table_name}"'
            with self.connection.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()[0]
                return result.isoformat() if result else "None"
        except Exception as e:
            self._logger.error(f"get_max_updated_at failed: {e}")
            self.rollback()
            return "Error"
        
    def export_diff_table(self, tables, last_backup_time: datetime, outpath: Path, basis: str) -> dict:
        print(Fore.YELLOW + f"Exporting differential data since {last_backup_time} using '{basis}'" + Style.RESET_ALL)

        outpath = Path(outpath) if isinstance(outpath, str) else outpath
        diff_root = outpath / ".backup_diff"
        diff_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        diff_dir = diff_root / diff_timestamp

        if not self._prepare_output_directory(diff_dir):
            return {}

        manifest_path = diff_root / "manifest.json"
        manifest_data = {
            "base_backup": last_backup_time.isoformat(),
            "diff_chain": [],
            "last_diff_timestamp": None
        }

        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest_data = json.load(f)
            except Exception as e:
                self._logger.warning(f"Failed to read existing manifest, creating new: {e}")

        exported_files = {}

        for schema, table_name in tables:
            print(Fore.CYAN + f"Last backup (UTC): {last_backup_time}" + Style.RESET_ALL)
            print(Fore.CYAN + f"Last row in DB: {self.get_max_updated_at(table_name, schema, basis)}" + Style.RESET_ALL)
            if not self._column_exists(schema, table_name, basis):
                print(Fore.YELLOW + f"Skipping {table_name}: column '{basis}' does not exist" + Style.RESET_ALL)
                self._logger.warning(f"Table {table_name} skipped: no '{basis}' column")
                continue

            file_path = diff_dir / f"{table_name}_diff.csv"
            try:
                # Parameterised query
                query = f'SELECT * FROM "{schema}"."{table_name}" WHERE {basis} > %s'
                with self.connection.cursor() as cur:
                    cur.execute(query, (last_backup_time,))
                    rows = cur.fetchall()
                    if not rows:
                        print(Fore.CYAN + f"No new rows in {table_name} since last backup" + Style.RESET_ALL)
                        continue

                    columns = [d[0] for d in cur.description]
                    self._write_table_to_csv(file_path, columns, rows)
                    file_size = file_path.stat().st_size

                    exported_files[table_name] = {
                        "table_name": table_name,
                        "file_path": str(file_path),
                        "rows_count": len(rows),
                        "file_size": file_size
                    }
                    print(Fore.GREEN + f"✓ Diff {table_name}: {len(rows)} rows → {file_path.name}" + Style.RESET_ALL)
                    self._logger.info(f"Diff export {table_name}: {len(rows)} rows, {file_size/1024:.2f} KB")

            except Exception as e:
                print(Fore.RED + f"✗ Diff export failed for {table_name}: {e}" + Style.RESET_ALL)
                self._logger.error(f"Diff export failed for {table_name}: {e}")

        if exported_files:
            manifest_data["diff_chain"].append(diff_timestamp)
            manifest_data["last_diff_timestamp"] = diff_timestamp
            try:
                with open(manifest_path, "w", encoding="utf-8") as f:
                    json.dump(manifest_data, f, indent=4, ensure_ascii=False)
                oschmod.set_mode(manifest_path, "600")
                print(Fore.CYAN + f"Manifest updated: {manifest_path}" + Style.RESET_ALL)
            except Exception as e:
                print(Fore.RED + f"Failed to update manifest: {e}" + Style.RESET_ALL)
                self._logger.error(f"Manifest update failed: {e}")

        return exported_files

    def perform_differential_backup(self, basis: str, tables: list = None):
        print(Fore.YELLOW + "Starting differential backup..." + Style.RESET_ALL)

        last_full_timestamp = self.get_last_full_backup_timestamp()
        backup_location = self.get_output_path_from_last_full_backup()

        if not last_full_timestamp or not backup_location:
            print(Fore.RED + "No previous full backup found. Cannot perform differential backup." + Style.RESET_ALL)
            return False

        if not tables:
            tables = self.get_table_names_from_last_full_backup()
            if not tables:
                print(Fore.RED + "No tables found in last full backup." + Style.RESET_ALL)
                return False
            tables = [("public", t) for t in tables]

        print(Fore.CYAN + f"Using basis column: {basis}" + Style.RESET_ALL)
        print(Fore.CYAN + f"Tables: {[t[1] for t in tables]}" + Style.RESET_ALL)

        result = self.export_diff_table(
            tables=tables,
            last_backup_time=last_full_timestamp,
            outpath=backup_location,
            basis=basis
        )

        if result:
            print(Fore.GREEN + "Differential backup completed successfully." + Style.RESET_ALL)
            return True
        else:
            print(Fore.RED + "Differential backup failed or no changes." + Style.RESET_ALL)
            return False
        
        
    def get_last_backup_path(self) -> str | None:
        catalog = BackupCatalog()
        last_backup = catalog.get_last_backup()
        if last_backup and last_backup.get("status") == "completed":
            return last_backup.get("backup_location")
        return None

    def compress_backup(self, path):
        """Compress a backup directory to zip."""
        path = Path(path) if isinstance(path, str) else path
        if not path.exists() or not path.is_dir():
            print(Fore.RED + f"Invalid path: {path}" + Style.RESET_ALL)
            return False
        try:
            zip_path = shutil.make_archive(str(path), 'zip', str(path))
            if zip_path:
                print(Fore.CYAN + "\n" + "="*60 + Style.RESET_ALL)
                print(Fore.GREEN + "Compressed backup location:" + Style.RESET_ALL)
                print(Fore.YELLOW + zip_path + Style.RESET_ALL)
                print(Fore.CYAN + "="*60 + "\n" + Style.RESET_ALL)
                return True
            print(Fore.YELLOW + "Compression produced no file" + Style.RESET_ALL)
            return False
        except Exception as e:
            print(Fore.RED + f"Compression failed: {e}" + Style.RESET_ALL)
            return False
