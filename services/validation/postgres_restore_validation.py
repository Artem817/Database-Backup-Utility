from __future__ import annotations

import shutil
import socket
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.restore.postgres_restore import PostgresRestoreService


RESTORE_VALIDATION_SCHEMA_VERSION = "1.0"


class PostgresRestoreValidationService:
    """
    Runs an automated PostgreSQL test-restore and compares restored table
    signatures against the live database connection used for the backup.
    """

    def __init__(
        self,
        backup_path: str | Path,
        archive_dir: str | Path,
        source_connection_params: dict[str, Any],
        restore_port: int = 0,
        verify_database: str | None = None,
        verify_user: str | None = None,
        pg_bin_dir: str | Path | None = None,
        checksum_tables: bool = True,
        keep_data_dir: bool = False,
        logger=None,
        messenger=None,
    ) -> None:
        self._backup_path = Path(backup_path).expanduser().resolve()
        self._archive_dir = Path(archive_dir).expanduser().resolve()
        self._source_connection_params = dict(source_connection_params)
        self._restore_port = int(restore_port)
        self._verify_database = (
            verify_database
            or self._source_connection_params.get("database")
            or self._source_connection_params.get("dbname")
        )
        self._verify_user = (
            verify_user
            or self._source_connection_params.get("user")
            or "postgres"
        )
        self._pg_bin_dir = Path(pg_bin_dir).expanduser().resolve() if pg_bin_dir else None
        self._checksum_tables = checksum_tables
        self._keep_data_dir = keep_data_dir
        self._logger = logger
        self._messenger = messenger

    def validate(self) -> dict[str, Any]:
        started_at = self._utc_now()
        report = self._build_base_report(started_at)
        temp_root: Path | None = None
        restore_service: PostgresRestoreService | None = None

        try:
            if not self._verify_database:
                raise ValueError("Target database name is required for restore validation")

            self._message_info("Collecting source table signatures")
            source_snapshot = self._collect_table_signatures(
                self._build_source_connect_kwargs()
            )
            report["source"] = source_snapshot
            report["checks"]["source_snapshot"] = {
                "passed": source_snapshot["state"] == "passed",
                "tables_checked": len(source_snapshot["tables"]),
            }

            temp_root = Path(tempfile.mkdtemp(prefix="dbtool_pg_restore_validation_"))
            data_dir = temp_root / "data"
            selected_port = self._select_restore_port()
            report["temporary_instance"].update(
                {
                    "data_dir": str(data_dir),
                    "port": selected_port,
                    "keep_data_dir": self._keep_data_dir,
                }
            )

            restore_service = PostgresRestoreService(
                backup_path=str(self._backup_path),
                data_dir=str(data_dir),
                archive_dir=str(self._archive_dir),
                restore_port=selected_port,
                verify_database=self._verify_database,
                verify_user=self._verify_user,
                verify_query="SELECT 1;",
                pg_bin_dir=str(self._pg_bin_dir) if self._pg_bin_dir else None,
                logger=self._logger,
                messenger=self._messenger,
            )

            self._message_info(
                f"Running automated test-restore on port {selected_port}"
            )
            restored = restore_service.restore()
            report["checks"]["temporary_restore"] = {"passed": restored}
            if not restored:
                report["state"] = "failed"
                report["error"] = "Temporary PostgreSQL restore failed"
                return report

            self._message_info("Collecting restored table signatures")
            restored_snapshot = self._collect_table_signatures(
                self._build_restored_connect_kwargs(selected_port)
            )
            report["restored"] = restored_snapshot
            report["checks"]["restored_snapshot"] = {
                "passed": restored_snapshot["state"] == "passed",
                "tables_checked": len(restored_snapshot["tables"]),
            }

            comparison = self._compare_snapshots(source_snapshot, restored_snapshot)
            report["checks"]["table_comparison"] = comparison
            report["state"] = "passed" if comparison["matches"] else "failed"
            if report["state"] == "passed":
                self._message_success("Automated restore validation passed")
            else:
                self._message_error("Automated restore validation failed")
        except Exception as error:
            report["state"] = "failed"
            report["error"] = str(error)
            self._message_error(f"Automated restore validation failed: {error}")
        finally:
            if restore_service is not None:
                report["temporary_instance"]["stopped"] = restore_service.stop()

            if temp_root is not None and temp_root.exists() and not self._keep_data_dir:
                shutil.rmtree(temp_root, ignore_errors=True)
                report["temporary_instance"]["cleaned_up"] = not temp_root.exists()

            report["completed_at"] = self._utc_now()
            report["duration_seconds"] = self._duration_seconds(
                report["started_at"],
                report["completed_at"],
            )

        return report

    def _build_base_report(self, started_at: str) -> dict[str, Any]:
        return {
            "schema_version": RESTORE_VALIDATION_SCHEMA_VERSION,
            "mode": "postgresql_test_restore",
            "state": "not_tested",
            "started_at": started_at,
            "completed_at": None,
            "duration_seconds": None,
            "backup_path": str(self._backup_path),
            "archive_dir": str(self._archive_dir),
            "database": self._verify_database,
            "checksum_tables": self._checksum_tables,
            "temporary_instance": {
                "data_dir": None,
                "port": None,
                "pg_bin_dir": str(self._pg_bin_dir) if self._pg_bin_dir else None,
                "keep_data_dir": self._keep_data_dir,
                "stopped": None,
                "cleaned_up": False,
            },
            "checks": {
                "source_snapshot": {"passed": False, "tables_checked": 0},
                "temporary_restore": {"passed": False},
                "restored_snapshot": {"passed": False, "tables_checked": 0},
                "table_comparison": {
                    "matches": False,
                    "matched_tables": 0,
                    "missing_tables": [],
                    "unexpected_tables": [],
                    "row_count_mismatches": [],
                    "checksum_mismatches": [],
                },
            },
            "source": {"state": "not_collected", "tables": []},
            "restored": {"state": "not_collected", "tables": []},
            "error": None,
        }

    def _build_source_connect_kwargs(self) -> dict[str, Any]:
        params = self._source_connection_params
        connect_kwargs: dict[str, Any] = {
            "dbname": params.get("database") or params.get("dbname"),
            "user": params.get("user"),
            "host": params.get("host") or "localhost",
            "port": int(params.get("port") or 5432),
            "connect_timeout": 10,
        }

        if not params.get("use_pgpass") and params.get("password"):
            connect_kwargs["password"] = params["password"]

        return connect_kwargs

    def _build_restored_connect_kwargs(self, selected_port: int) -> dict[str, Any]:
        return {
            "dbname": self._verify_database,
            "user": self._verify_user,
            "host": "127.0.0.1",
            "port": selected_port,
            "connect_timeout": 10,
        }

    def _collect_table_signatures(self, connect_kwargs: dict[str, Any]) -> dict[str, Any]:
        import psycopg2

        snapshot = {
            "state": "passed",
            "collected_at": self._utc_now(),
            "algorithm": "row_count+md5_ordered_row_json_hash",
            "tables": [],
            "errors": [],
        }

        with psycopg2.connect(**connect_kwargs) as connection:
            connection.autocommit = True
            tables = self._list_user_tables(connection)

            for table_ref in tables:
                table_signature = {
                    "schema": table_ref["schema"],
                    "table": table_ref["table"],
                    "row_count": None,
                    "checksum": None,
                    "state": "passed",
                    "error": None,
                }
                try:
                    table_signature["row_count"] = self._count_table(
                        connection,
                        table_ref["schema"],
                        table_ref["table"],
                    )
                    if self._checksum_tables:
                        table_signature["checksum"] = self._checksum_table(
                            connection,
                            table_ref["schema"],
                            table_ref["table"],
                        )
                except Exception as error:
                    table_signature["state"] = "failed"
                    table_signature["error"] = str(error)
                    snapshot["state"] = "failed"
                    snapshot["errors"].append(
                        {
                            "schema": table_ref["schema"],
                            "table": table_ref["table"],
                            "error": str(error),
                        }
                    )

                snapshot["tables"].append(table_signature)

        return snapshot

    def _list_user_tables(self, connection) -> list[dict[str, str]]:
        query = """
            SELECT n.nspname AS schema_name, c.relname AS table_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname NOT LIKE 'pg_toast%'
            ORDER BY n.nspname, c.relname
        """
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

        return [{"schema": row[0], "table": row[1]} for row in rows]

    def _count_table(self, connection, schema_name: str, table_name: str) -> int:
        from psycopg2 import sql

        query = sql.SQL("SELECT COUNT(*)::bigint FROM {}").format(
            sql.Identifier(schema_name, table_name)
        )
        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
        return int(row[0])

    def _checksum_table(self, connection, schema_name: str, table_name: str) -> str:
        from psycopg2 import sql

        query = sql.SQL(
            """
            SELECT COALESCE(md5(string_agg(row_hash, '' ORDER BY row_hash)), md5(''))
            FROM (
                SELECT md5(row_to_json(t)::text) AS row_hash
                FROM {} AS t
            ) AS hashed_rows
            """
        ).format(sql.Identifier(schema_name, table_name))
        with connection.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
        return str(row[0])

    def _compare_snapshots(
        self,
        source_snapshot: dict[str, Any],
        restored_snapshot: dict[str, Any],
    ) -> dict[str, Any]:
        source_tables = self._table_map(source_snapshot)
        restored_tables = self._table_map(restored_snapshot)

        source_keys = set(source_tables)
        restored_keys = set(restored_tables)
        missing_tables = sorted(source_keys - restored_keys)
        unexpected_tables = sorted(restored_keys - source_keys)
        row_count_mismatches: list[dict[str, Any]] = []
        checksum_mismatches: list[dict[str, Any]] = []
        table_results: list[dict[str, Any]] = []

        for table_key in sorted(source_keys & restored_keys):
            source_table = source_tables[table_key]
            restored_table = restored_tables[table_key]
            row_count_matches = (
                source_table.get("row_count") == restored_table.get("row_count")
            )
            checksum_matches = (
                not self._checksum_tables
                or source_table.get("checksum") == restored_table.get("checksum")
            )

            if not row_count_matches:
                row_count_mismatches.append(
                    {
                        "table": table_key,
                        "source": source_table.get("row_count"),
                        "restored": restored_table.get("row_count"),
                    }
                )
            if not checksum_matches:
                checksum_mismatches.append(
                    {
                        "table": table_key,
                        "source": source_table.get("checksum"),
                        "restored": restored_table.get("checksum"),
                    }
                )

            table_results.append(
                {
                    "table": table_key,
                    "source_row_count": source_table.get("row_count"),
                    "restored_row_count": restored_table.get("row_count"),
                    "source_checksum": source_table.get("checksum"),
                    "restored_checksum": restored_table.get("checksum"),
                    "matches": row_count_matches and checksum_matches,
                }
            )

        snapshots_passed = (
            source_snapshot.get("state") == "passed"
            and restored_snapshot.get("state") == "passed"
        )
        matches = (
            snapshots_passed
            and not missing_tables
            and not unexpected_tables
            and not row_count_mismatches
            and not checksum_mismatches
        )

        return {
            "matches": matches,
            "matched_tables": len(source_keys & restored_keys),
            "source_table_count": len(source_keys),
            "restored_table_count": len(restored_keys),
            "missing_tables": missing_tables,
            "unexpected_tables": unexpected_tables,
            "row_count_mismatches": row_count_mismatches,
            "checksum_mismatches": checksum_mismatches,
            "tables": table_results,
        }

    @staticmethod
    def _table_map(snapshot: dict[str, Any]) -> dict[str, dict[str, Any]]:
        return {
            f"{table['schema']}.{table['table']}": table
            for table in snapshot.get("tables", [])
        }

    def _select_restore_port(self) -> int:
        if self._restore_port > 0:
            return self._restore_port

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as candidate:
            candidate.bind(("127.0.0.1", 0))
            return int(candidate.getsockname()[1])

    @staticmethod
    def _utc_now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _duration_seconds(started_at: str, completed_at: str) -> float:
        start = datetime.fromisoformat(started_at)
        end = datetime.fromisoformat(completed_at)
        return (end - start).total_seconds()

    def _message_info(self, message: str) -> None:
        if self._messenger is not None:
            self._messenger.info(message)
        if self._logger is not None:
            self._logger.info(message)

    def _message_success(self, message: str) -> None:
        if self._messenger is not None:
            self._messenger.success(message)
        if self._logger is not None:
            self._logger.info(message)

    def _message_error(self, message: str) -> None:
        if self._messenger is not None:
            self._messenger.error(message)
        if self._logger is not None:
            self._logger.error(message)
