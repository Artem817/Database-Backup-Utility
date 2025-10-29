import csv
import os
import subprocess
from pathlib import Path

from services.interfaces import IConnectionProvider, IMessenger, ILogger


class SchemaExporter:
    def __init__(self, 
                 connection_provider: IConnectionProvider,
                 logger: ILogger, messenger: IMessenger):
        self._connection_provider = connection_provider
        self._logger = logger
        self._messenger = messenger
    
    def export_schema(self, output_path: Path) -> str | None:
        try:
            if not output_path.parent.exists():
                output_path.parent.mkdir(parents=True, exist_ok=True)
                
            connection_params = self._connection_provider.get_connection_params()
            command = [
                "pg_dump",
                "--host", connection_params["host"],
                "--port", str(connection_params["port"]),
                "--username", connection_params["user"],
                "--dbname", connection_params["database"],
                "--schema-only",
            ]
            
            env = os.environ.copy()
            env["PGPASSWORD"] = connection_params["password"]
            
            with open(output_path, "w", encoding="utf-8") as f:
                subprocess.run(command, stdout=f, check=True, env=env)
                
            self._messenger.success(f"Schema exported: {output_path}")
            return str(output_path)
            
        except Exception as e:
            self._messenger.error(f"Schema export failed: {e}")
            self._logger.error(f"Schema export error: {e}")
            return None
        
    def get_table_schema(self, table_name: str, schema: str = "public"):
        try:
            connection = self._connection_provider.get_connection()
            with connection.cursor() as cur:
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
            connection = self._connection_provider.get_connection()
            connection_params = self._connection_provider.get_connection_params()
            with connection.cursor() as cur:
                cur.execute("SELECT pg_size_pretty(pg_database_size(%s));", (connection_params["database"],))
                size = cur.fetchone()[0]
                self._messenger.success(f"Database size: {size}")
                return size
        except Exception as e:
            self._messenger.error(f"Failed to get DB size: {e}")
            return "Unknown"
    
    def table_exists(self, table_name: str, schema: str = "public") -> bool:
        connection = self._connection_provider.get_connection()
        with connection.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                );
            """, (schema, table_name))
            return cur.fetchone()[0]
    
    def _column_exists(self, schema: str, table_name: str, column: str) -> bool:
        try:
            connection = self._connection_provider.get_connection()
            with connection.cursor() as cur:
                cur.execute("SAVEPOINT check_column")
                try:
                    cur.execute("""
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s AND column_name = %s;
                    """, (schema, table_name, column))
                    result = cur.fetchone() is not None
                    cur.execute("RELEASE SAVEPOINT check_column")
                    return result
                except Exception:
                    cur.execute("ROLLBACK TO SAVEPOINT check_column")
                    return False
        except Exception as e:
            self._logger.error(f"Column check failed: {e}")
            return False



class TableExporter:
    def __init__(self, 
                 connection_provider: IConnectionProvider,
                 logger: ILogger,
                 messenger: IMessenger):
        self._connection_provider = connection_provider
        self._logger = logger
        self._messenger = messenger
        
    def get_tables(self):
        """Return list of (schema, table_name) for user tables."""
        connection = self._connection_provider.get_connection()
        with connection.cursor() as cur:
            cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE'
              AND table_schema NOT IN ('pg_catalog', 'information_schema');
            """)
            return cur.fetchall()
    
    def export_table(self, schema: str, table_name: str, 
                    file_path: Path, where: str = None) -> dict | None:
        try:
            connection = self._connection_provider.get_connection()
            full_table_name = f'"{schema}"."{table_name}"'
            
            query = f"SELECT * FROM {full_table_name}"
            if where:
                query += f" WHERE {where}"
                
            with connection.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                columns = [d[0] for d in cur.description]
                
                self._write_to_csv(file_path, columns, rows)
                file_size = file_path.stat().st_size
                
                self._messenger.success(
                    f"Exported {table_name}: {len(rows)} rows, {file_size/1024:.2f} KB"
                )
                
                return {
                    "table_name": table_name,
                    "file_path": str(file_path),
                    "rows_count": len(rows),
                    "file_size": file_size
                }
                
        except Exception as e:
            self._messenger.error(f"Export {table_name} failed: {e}")
            self._logger.error(f"Table export failed for {table_name}: {e}")
            return None
    
    def _write_to_csv(self, file_path: Path, columns: list, rows: list):
        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)
