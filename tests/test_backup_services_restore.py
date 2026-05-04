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
        self.backup_full_calls = []

    def backup_full(self, **kwargs):
        self.backup_full_calls.append(kwargs)


class BackupServiceRestoreTests(unittest.TestCase):
    def test_full_backup_passes_restore_validation_flags_for_postgres(self) -> None:
        dbclient = DummyDbClient()
        service = BackupService(dbclient)
        parsed_args = SimpleNamespace(
            path="/backups",
            storage_type="local",
            single_archive=True,
            validate_restore=True,
            restore_validation_port=55440,
            restore_validation_keep_data=True,
            restore_validation_checksums=False,
            pg_bin_dir="/opt/postgres/bin",
        )

        service.full_backup(parsed_args)

        self.assertEqual(
            dbclient.backup_full_calls,
            [
                {
                    "outpath": "/backups",
                    "single_archive": True,
                    "storage": "local",
                    "validate_restore": True,
                    "restore_validation_port": 55440,
                    "restore_validation_keep_data": True,
                    "restore_validation_checksums": False,
                    "pg_bin_dir": "/opt/postgres/bin",
                }
            ],
        )

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
            target_time=None,
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
                target_time=None,
                logger=service.dbclient._logger,
                messenger=service.dbclient._messenger,
            )

    def test_restore_postgres_selects_catalog_base_for_target_time(self) -> None:
        service = BackupService(DummyDbClient())
        parsed_args = SimpleNamespace(
            backup_path=None,
            path=None,
            data_dir="/tmp/restore-data",
            wal_path="/manual/wal-archive",
            archive_dir=None,
            restore_port=55432,
            verify_user=None,
            verify_query="SELECT 1;",
            pg_bin_dir=None,
            target_time="2026-05-04 12:30:00",
            catalog_path="/tmp/catalog.db",
        )

        with patch("services.backup_services.PostgresPitrPlanner") as planner_cls:
            with patch("services.backup_services.PostgresRestoreService") as restore_cls:
                planner_cls.return_value.build_plan.return_value = SimpleNamespace(
                    backup_path="/backups/full_pg",
                    recovery_target_time="2026-05-04 12:30:00+00:00",
                )
                restore_cls.return_value.restore.return_value = True

                service.restore_postgres(parsed_args)

                planner_cls.assert_called_once_with(
                    database_name="BackUpTestDB",
                    target_time="2026-05-04 12:30:00",
                    catalog_path="/tmp/catalog.db",
                    messenger=service.dbclient._messenger,
                )
                restore_cls.assert_called_once_with(
                    backup_path="/backups/full_pg",
                    data_dir="/tmp/restore-data",
                    archive_dir="/manual/wal-archive",
                    restore_port=55432,
                    verify_database="BackUpTestDB",
                    verify_user="artempavlenko",
                    verify_query="SELECT 1;",
                    pg_bin_dir=None,
                    target_time="2026-05-04 12:30:00+00:00",
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
            target_time=None,
        )

        with self.assertRaisesRegex(ValueError, "only for PostgreSQL"):
            service.restore_postgres(parsed_args)


if __name__ == "__main__":
    unittest.main()
