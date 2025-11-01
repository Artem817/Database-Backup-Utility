import json
import shutil
from datetime import datetime
from pathlib import Path
import subprocess
import os

import oschmod

from services.interfaces import IConnectionProvider, ILogger, IMessenger

class CompressionService:
    def __init__(self, logger: ILogger, messenger: IMessenger):
        self._logger = logger
        self._messenger = messenger

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

class DifferentialBackupService:
    def __init__(self,
                 connection_provider: IConnectionProvider,
                 logger: ILogger,
                 messenger: IMessenger,
                 schema_exporter,
                 table_exporter):
        self._connection_provider = connection_provider
        self._logger = logger
        self._messenger = messenger
        self._schema_exporter = schema_exporter
        self._table_exporter = table_exporter

    def get_max_updated_at(self, table_name: str, schema: str, column: str):
        try:
            connection_params = self._connection_provider.get_connection_params()
            
            if connection_params.get("port") == 3306:
                query = f'SELECT MAX(`{column}`) FROM `{table_name}`'
            else:
                query = f'SELECT MAX({column}) FROM "{schema}"."{table_name}"'
                
            connection = self._connection_provider.get_connection()
            with connection.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()
                
                if connection_params.get("port") == 3306:
                    result_value = result[f'MAX(`{column}`)'] if isinstance(result, dict) else result[0]
                else:
                    result_value = result[0]
                    
                return result_value.isoformat() if result_value else "None"
        except Exception as e:
            self._logger.error(f"get_max_updated_at failed: {e}")
            if hasattr(self._connection_provider.get_connection(), 'rollback'):
                self._connection_provider.get_connection().rollback()
            return "Error"

    def export_diff_table_zstd(self, tables, last_backup_time: datetime, outpath: Path, basis: str) -> dict:
        """Export differential backup using native database utilities with zstd compression"""
        self._messenger.warning(f"Exporting differential data since {last_backup_time} using '{basis}'")

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

        connection_params = self._connection_provider.get_connection_params()
        database_name = connection_params["database"]
        
        diff_filename = f"{database_name}_diff_{diff_timestamp}.sql.zst"
        diff_file_path = diff_dir / diff_filename
        
        where_conditions = []
        valid_tables = []
        
        for schema, table_name in tables:
            if not self._schema_exporter._column_exists(schema, table_name, basis):
                self._messenger.warning(f"Skipping {table_name}: column '{basis}' does not exist")
                continue
            valid_tables.append((schema, table_name))
            
        if not valid_tables:
            self._messenger.warning("No valid tables found for differential backup")
            return {}

        try:
            if connection_params.get("port") == 3306:
                # MySQL differential backup
                success = self._create_mysql_differential_backup(
                    valid_tables, last_backup_time, diff_file_path, basis, connection_params
                )
            else:
                # PostgreSQL differential backup  
                success = self._create_postgres_differential_backup(
                    valid_tables, last_backup_time, diff_file_path, basis, connection_params
                )

            if success:
                file_size = diff_file_path.stat().st_size
                self._messenger.success(f"Differential backup created: {diff_file_path}")
                self._logger.info(f"Differential backup file: {diff_file_path} ({file_size/1024:.2f} KB)")
                
                # Update manifest
                manifest_data["diff_chain"].append(diff_timestamp)
                manifest_data["last_diff_timestamp"] = diff_timestamp
                with open(manifest_path, "w", encoding="utf-8") as f:
                    json.dump(manifest_data, f, indent=4, ensure_ascii=False)
                oschmod.set_mode(manifest_path, "600")
                
                return {
                    "differential_backup": {
                        "file_path": str(diff_file_path),
                        "file_size": file_size,
                        "tables_count": len(valid_tables),
                        "timestamp": diff_timestamp
                    }
                }
            else:
                return {}
                
        except Exception as e:
            self._messenger.error(f"Differential backup failed: {e}")
            self._logger.error(f"Differential backup failed: {e}")
            return {}

    def _create_postgres_differential_backup(self, tables, last_backup_time, output_file, basis, connection_params):
        """Create PostgreSQL differential backup using pg_dump with WHERE conditions"""
        
        temp_sql = output_file.parent / f"temp_diff_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        
        try:
            with open(temp_sql, 'w') as f:
                for schema, table_name in tables:
                    f.write(f"-- Differential data for {schema}.{table_name}\n")
                    f.write(f"COPY (SELECT * FROM \"{schema}\".\"{table_name}\" WHERE {basis} > '{last_backup_time.isoformat()}') TO STDOUT;\n")
            
            pg_dump_cmd = [
                "pg_dump",
                "-h", connection_params["host"],
                "-p", str(connection_params["port"]),
                "-U", connection_params["user"],
                "-d", connection_params["database"],
                "--data-only"
            ]
            
            for schema, table_name in tables:
                pg_dump_cmd.extend(["-t", f"{schema}.{table_name}"])
            
            zstd_cmd = [
                "zstd",
                "-o", str(output_file),
                "-"
            ]
            
            env = os.environ.copy()
            env['PGPASSWORD'] = connection_params["password"]
            
            pg_dump_process = subprocess.Popen(
                pg_dump_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=env
            )

            zstd_process = subprocess.Popen(
                zstd_cmd,
                stdin=pg_dump_process.stdout,
                stderr=subprocess.PIPE
            )
            
            pg_dump_process.stdout.close()
            pg_dump_process.wait()
            zstd_process.wait()
            
            # Clean up temp file
            if temp_sql.exists():
                temp_sql.unlink()
            
            if pg_dump_process.returncode == 0 and zstd_process.returncode == 0:
                return True
            else:
                self._logger.error("pg_dump or zstd failed in differential backup")
                return False
                
        except Exception as e:
            self._logger.error(f"PostgreSQL differential backup failed: {e}")
            if temp_sql.exists():
                temp_sql.unlink()
            return False

    def _create_mysql_differential_backup(self, tables, last_backup_time, output_file, basis, connection_params):
        """Create MySQL differential backup using mysqldump with WHERE conditions"""
        
        try:
            
            mysqldump_cmd = [
                "mysqldump",
                "--host", connection_params["host"],
                "--port", str(connection_params["port"]),
                "--user", connection_params["user"],
                f"--password={connection_params['password']}",
                "--single-transaction",
                "--no-create-info",  # data only
                connection_params["database"]
            ]
            
            table_names = [table_name for _, table_name in tables]
            mysqldump_cmd.extend(table_names)
            
            where_clause = f"--where={basis} > '{last_backup_time.isoformat()}'"
            mysqldump_cmd.append(where_clause)
            
            zstd_cmd = [
                "zstd",
                "-o", str(output_file),
                "-"
            ]
            
            mysqldump_process = subprocess.Popen(
                mysqldump_cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            zstd_process = subprocess.Popen(
                zstd_cmd,
                stdin=mysqldump_process.stdout,
                stderr=subprocess.PIPE
            )
            
            mysqldump_process.stdout.close()
            mysqldump_process.wait()
            zstd_process.wait()
            
            if mysqldump_process.returncode == 0 and zstd_process.returncode == 0:
                return True
            else:
                self._logger.error("mysqldump or zstd failed in differential backup")
                return False
                
        except Exception as e:
            self._logger.error(f"MySQL differential backup failed: {e}")
            return False

    def export_diff_table(self, tables, last_backup_time: datetime, outpath: Path, basis: str) -> dict:
        """Wrapper method to maintain compatibility - now uses zstd"""
        return self.export_diff_table_zstd(tables, last_backup_time, outpath, basis)

    def _prepare_output_directory(self, outpath: Path) -> bool:
        try:
            outpath.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            self._messenger.error(f"Failed to create {outpath}: {e}")
            self._logger.error(f"Dir creation failed: {e}")
            return False

    def perform_differential_backup(self, basis: str, metadata_reader, tables: list = None):
        self._messenger.warning("Starting differential backup...")

        last_full_timestamp = metadata_reader.get_last_full_backup_timestamp()
        backup_location = metadata_reader.get_output_path_from_last_full_backup()

        if not last_full_timestamp or not backup_location:
            self._messenger.error("No previous full backup found. Cannot perform differential backup.")
            return False

        if not tables:
            tables = metadata_reader.get_table_names_from_last_full_backup()
            
            if not tables:
                self._messenger.info("No tables found in backup metadata, fetching from database...")
                tables = self._table_exporter.get_tables()
                if not tables:
                    self._messenger.error("No tables found in database.")
                    return False
                if tables and isinstance(tables[0], tuple):
                    tables = [table[1] for table in tables]  
            
            connection_params = self._connection_provider.get_connection_params()
            if connection_params.get("port") == 3306:
                tables = [(connection_params["database"], t) for t in tables]
            else:
                tables = [("public", t) for t in tables]

        self._messenger.info(f"Using basis column: {basis}")
        self._messenger.info(f"Tables: {[t[1] for t in tables]}")

        result = self.export_diff_table(
            tables=tables,
            last_backup_time=last_full_timestamp,
            outpath=backup_location,
            basis=basis
        )

        if result:
            self._messenger.success("Differential backup completed successfully.")
            return True
        else:
            self._messenger.error("Differential backup failed or no changes.")
            return False
