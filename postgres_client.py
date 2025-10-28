import csv
from pathlib import Path
import psycopg2
import sqlparse
from psycopg2.extensions import connection
from datetime import datetime
import os
import subprocess
import json
import shutil
import sys

from factory import DatabaseClient
from custom_logging import BackupLogger, BackupCatalog
from console_utils import get_messenger

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
        self._messenger = get_messenger()

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
                self._messenger.success("PostgreSQL connection successful!")
                self._messenger.info(f"  Server version: {self._database_version}")
                self._logger.info(f"Connected to database: {self._database} ({self._database_version})")
            return self._connection
        except psycopg2.OperationalError as e:
            self._messenger.error(f"Unable to connect. Details: {e}")
            self._messenger.warning("Check your .env/CLI settings.")
            self._logger.error(f"Connection failed: {e}")
            return None

    def disconnect(self):
        try:
            if self._connection and not self._connection.closed:
                self._connection.close()
                self._connection = None
                self._messenger.info("Disconnected from database.")
                self._logger.info("Database connection closed")
        except psycopg2.OperationalError as e:
            self._messenger.error(f"Error on disconnect: {e}")
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
            self._messenger.error(f"Failed to get schema for {table_name}: {e}")
            self._logger.error(f"Schema retrieval failed for {table_name}: {e}")
            return []

    def get_database_size(self) -> str:
        try:
            with self.connection.cursor() as cur:
                cur.execute("SELECT pg_size_pretty(pg_database_size(%s));", (self._database,))
                size = cur.fetchone()[0]
                self._messenger.success(f"Database size: {size}")
                return size
        except Exception as e:
            self._messenger.error(f"Failed to get DB size: {e}")
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

    def _create_backup_structure(self, base_path: Path, backup_id: str) -> dict:
        backup_root = base_path / backup_id
        data_dir = backup_root / "data"
        backup_root.mkdir(parents=True, exist_ok=True)
        data_dir.mkdir(parents=True, exist_ok=True)
        return {
            "root": backup_root,
            "data": data_dir,
            "schema": backup_root / "schema.sql",
            "metadata": backup_root / "metadata.json",
        }

    def export_table(self, tables, outpath, metadata=None) -> list[dict]:
        saved_files = []
        outpath = Path(outpath) if isinstance(outpath, str) else outpath
        try:
            outpath.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self._messenger.error(f"Failed to create {outpath}: {e}")
            self._logger.error(f"Dir creation failed: {e}")
            return []

        try:
            with self.connection.cursor() as cur:
                for schema, table_name in tables:
                    full_table_name = f'"{schema}"."{table_name}"'
                    file_path = outpath / f"{table_name}.csv"
                    try:
                        cur.execute(f"SELECT * FROM {full_table_name};")
                        rows = cur.fetchall()
                        columns = [d[0] for d in cur.description]
                        with file_path.open("w", newline="", encoding="utf-8") as f:
                            writer = csv.writer(f)
                            writer.writerow(columns)
                            writer.writerows(rows)
                        file_size = file_path.stat().st_size
                        saved_files.append({
                            "table_name": table_name,
                            "file_path": str(file_path),
                            "rows_count": len(rows),
                            "file_size": file_size
                        })
                        if metadata:
                            self._logger.log_table_backup(
                                metadata=metadata,
                                table_name=table_name,
                                rows_count=len(rows),
                                file_size=file_size,
                                file_path=str(file_path)
                            )
                        self._messenger.success(f"Saved: {file_path.name} ({len(rows)} rows, {file_size / 1024:.2f} KB)")
                    except Exception as e:
                        self._messenger.error(f"Export {table_name} failed: {e}")
                        self._logger.error(f"Table export failed for {table_name}: {e}")
                        continue
            return saved_files
        except Exception as e:
            self._messenger.error(f"Export failed: {e}")
            self._logger.error(f"Export op failed: {e}")
            return []

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
            self._messenger.success(f"Schema exported: {outpath}")
            return str(outpath)
        except subprocess.CalledProcessError as e:
            self._messenger.error(f"pg_dump failed: {e}")
            self._logger.error(f"pg_dump failed: {e}")
            return None
        except Exception as e:
            self._messenger.error(f"Schema export error: {e}")
            self._logger.error(f"Schema export failed: {e}")
            return None

    def _save_metadata(self, metadata: dict, filepath: Path):
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=4, ensure_ascii=False)
            self._messenger.success(f"Metadata saved: {filepath}")
        except Exception as e:
            self._messenger.error(f"Failed to save metadata: {e}")
            self._logger.error(f"Metadata save failed: {e}")

    def backup_full(self, outpath, type: str = "csv", compress: bool = False):
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        self._messenger.info(f"Starting full backup → {base_path}")
        if compress:
            self._messenger.info("Compression enabled")

        metadata = self._logger.start_backup(
            backup_type="full",
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version=self._utility_version,
            compress=compress
        )

        try:
            backup_structure = self._create_backup_structure(base_path, metadata["id"])
            self._messenger.info(f"Backup dir: {backup_structure['root']}")

            schema_path = self.database_schema(backup_structure["schema"])
            if schema_path:
                metadata["schema_file"] = str(backup_structure["schema"])

            tables = self.get_tables()
            if not tables:
                self._messenger.warning("No tables found")
                self._logger.warning("No tables for backup")
                self._logger.finish_backup(metadata, success=False)
                return False

            self._messenger.info(f"Found {len(tables)} table(s)...")
            export = self.export_table(tables, backup_structure["data"], metadata=metadata)
            if not export:
                self._messenger.error("Backup failed - no files exported")
                self._logger.finish_backup(metadata, success=False)
                return False

            metadata["backup_location"] = str(backup_structure["root"])
            self._logger.finish_backup(metadata, success=True)
            self._save_metadata(metadata, backup_structure["metadata"])

            if compress:
                self._messenger.info("Compressing...")
                self.compress_backup(backup_structure['root'])

            self._messenger.success("Full backup completed")
            return True
        except Exception as e:
            self._messenger.error(f"Backup failed: {e}")
            self._logger.error(f"Backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False

    def partial_backup(self, tables: list, outpath: str, backup_type: str = "partial", compress: bool = False):
        base_path = Path(outpath) if isinstance(outpath, str) else outpath
        self._messenger.info(f"Starting {backup_type} backup → {base_path}")
        if compress:
            self._messenger.info("Compression enabled")

        metadata = self._logger.start_backup(
            backup_type=backup_type,
            database=self._database,
            database_version=self._database_version or "Unknown",
            utility_version=self._utility_version,
            compress=compress
        )

        try:
            backup_structure = self._create_backup_structure(base_path, metadata["id"])
            self._messenger.info(f"Backup dir: {backup_structure['root']}")

            schema_path = self.database_schema(backup_structure["schema"])
            if schema_path:
                metadata["schema_file"] = str(backup_structure["schema"])

            verified_tables = []
            for table in tables:
                if self.table_exists(table_name=table):
                    verified_tables.append(("public", table))
                    self._messenger.success(f"Table '{table}' found")
                    self._logger.info(f"Table '{table}' verified")
                else:
                    self._messenger.error(f"Table '{table}' doesn't exist")
                    self._logger.warning(f"Table '{table}' missing")

            if not verified_tables:
                self._messenger.warning("No valid tables to export")
                self._logger.finish_backup(metadata, success=False)
                return False

            export = self.export_table(verified_tables, backup_structure["data"], metadata=metadata)
            if not export:
                self._messenger.error("Backup failed - no files exported")
                self._logger.finish_backup(metadata, success=False)
                return False

            metadata["backup_location"] = str(backup_structure["root"])
            self._logger.finish_backup(metadata, success=True)
            self._save_metadata(metadata, backup_structure["metadata"])

            if compress:
                self._messenger.info("Compressing...")
                self.compress_backup(backup_structure['root'])

            self._messenger.success("Partial backup completed")
            return True
        except Exception as e:
            self._messenger.error(f"Backup failed: {e}")
            self._logger.error(f"Partial backup failed: {e}")
            self._logger.finish_backup(metadata, success=False)
            return False

    def execute_query(self, query: str):
        is_safe, message = analyze_sql(query)
        if not is_safe:
            self._messenger.warning(message)
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
                    self._messenger.success(f"Query executed. {affected} rows affected.")
                    self._logger.info(f"Query executed, {affected} rows affected")
                    return ([], [])
        except Exception as e:
            self._messenger.error(f"Query failed: {e}")
            self._logger.error(f"Query failed: {e}")
            self.connection.rollback()
            return None

    def csv_fragmental_backup(self, rows, outpath, query: str = None):
        try:
            if not rows or (isinstance(rows, tuple) and not rows[0]):
                self._messenger.warning("No data to export")
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
                self._messenger.error("Invalid data format for CSV export")
                self._logger.error("Invalid CSV export data format")
                return False

            with file_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(data)

            file_size = file_path.stat().st_size
            self._messenger.success(f"Saved: {file_path} ({len(data)} rows, {file_size / 1024:.2f} KB)")
            self._logger.info(f"Query result exported: {file_path} ({len(data)} rows, {file_size} bytes)")
            return str(file_path)
        except Exception as e:
            self._messenger.error(f"Failed to save query result: {e}")
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
            self._messenger.warning("No backup history found")
            return
        
        self._messenger.info(f"\n{'='*80}")
        self._messenger.info(f"Recent Backup History (last {limit})")
        self._messenger.info(f"{'='*80}")
        
        for backup in history:
            status_color = "success" if backup.get("status") == "completed" else "error"
            print(f"\nID: {backup.get('id')}")
            print(f"Type: {backup.get('type')}")
            print(f"Status: ", end="")
            if status_color == "success":
                self._messenger.success(backup.get('status'))
            else:
                self._messenger.error(backup.get('status'))
            print(f"Started: {backup.get('timestamp_start')}")
            print(f"Duration: {backup.get('duration_seconds', 0):.2f}s")
            stats = backup.get('statistics', {})
            if stats:
                size_mb = stats.get('total_size_bytes', 0) / 1024 / 1024
                print(f"Tables: {stats.get('total_tables', 0)}")
                print(f"Rows: {stats.get('total_rows_processed', 0)}")
                print(f"Size: {size_mb:.2f} MB")
        self._messenger.info(f"\n{'='*80}\n")

    def differential_backup(self):
        pass  # reserved for future

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
            self._messenger.error(f"Invalid path: {path}")
            return False
        try:
            zip_path = shutil.make_archive(str(path), 'zip', str(path))
            if zip_path:
                self._messenger.info("\n" + "="*60)
                self._messenger.success("Compressed backup location:")
                self._messenger.info(zip_path)
                self._messenger.info("="*60 + "\n")
                return True
            self._messenger.warning("Compression produced no file")
            return False
        except Exception as e:
            self._messenger.error(f"Compression failed: {e}")
            return False
