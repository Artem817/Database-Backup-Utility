from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from services.backup_services import BackupService


class DummyDbClient:
    def __init__(self, database_engine: str = "postgresql") -> None:
        self.database_engine = database_engine
        self.database_name = "BackUpTestDB"
        self.connection_params = {"user": "artempavlenko"}
        self.archive_path = "/configured/archive"
        self._logger = object()
        self._messenger = object()


class BackupServiceRestoreTests(unittest.TestCase):
    def test_restore_postgres_uses_manual_wal_path(self) -> None:
        service = BackupService(DummyDbClient())
        parsed_args = SimpleNamespace(
            backup_path=None,
            path="/backups/full_pg",
            data_dir="/tmp/restore-data",
            wal_path="/manual/wal-archive",
            archive_dir=None,
            restore_port=55432,
            verify_user=None,
            verify_query="SELECT 1;",
            pg_bin_dir=None,
        )

        with patch("services.backup_services.PostgresRestoreService") as restore_cls:
            restore_cls.return_value.restore.return_value = True

            service.restore_postgres(parsed_args)

            restore_cls.assert_called_once_with(
                backup_path="/backups/full_pg",
                data_dir="/tmp/restore-data",
                archive_dir="/manual/wal-archive",
                restore_port=55432,
                verify_database="BackUpTestDB",
                verify_user="artempavlenko",
                verify_query="SELECT 1;",
                pg_bin_dir=None,
                logger=service.dbclient._logger,
                messenger=service.dbclient._messenger,
            )

    def test_restore_postgres_rejects_non_postgres_client(self) -> None:
        service = BackupService(DummyDbClient(database_engine="mysql"))
        parsed_args = SimpleNamespace(
            backup_path="/backups/full_pg",
            path=None,
            data_dir="/tmp/restore-data",
            wal_path="/manual/wal-archive",
            archive_dir=None,
            restore_port=55432,
            verify_user=None,
            verify_query="SELECT 1;",
            pg_bin_dir=None,
        )

        with self.assertRaisesRegex(ValueError, "only for PostgreSQL"):
            service.restore_postgres(parsed_args)


if __name__ == "__main__":
    unittest.main()
