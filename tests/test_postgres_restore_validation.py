from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from services.validation.postgres_restore_validation import PostgresRestoreValidationService


class DummyLogger:
    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str) -> None:
        pass


class DummyMessenger:
    def info(self, message: str) -> None:
        pass

    def success(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str) -> None:
        pass


class PostgresRestoreValidationServiceTests(unittest.TestCase):
    def _build_service(self, backup_dir: Path, archive_dir: Path) -> PostgresRestoreValidationService:
        return PostgresRestoreValidationService(
            backup_path=backup_dir,
            archive_dir=archive_dir,
            source_connection_params={
                "host": "localhost",
                "port": 5432,
                "user": "postgres",
                "password": "",
                "database": "appdb",
                "use_pgpass": True,
            },
            restore_port=55440,
            verify_database="appdb",
            verify_user="postgres",
            logger=DummyLogger(),
            messenger=DummyMessenger(),
        )

    def test_validate_passes_when_restored_signatures_match_source(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backup_dir = root / "backup"
            archive_dir = root / "archive"
            backup_dir.mkdir()
            archive_dir.mkdir()

            source_snapshot = {
                "state": "passed",
                "tables": [
                    {
                        "schema": "public",
                        "table": "users",
                        "row_count": 3,
                        "checksum": "abc",
                    }
                ],
            }
            restored_snapshot = {
                "state": "passed",
                "tables": [
                    {
                        "schema": "public",
                        "table": "users",
                        "row_count": 3,
                        "checksum": "abc",
                    }
                ],
            }

            service = self._build_service(backup_dir, archive_dir)

            with patch.object(
                service,
                "_collect_table_signatures",
                side_effect=[source_snapshot, restored_snapshot],
            ), patch(
                "services.validation.postgres_restore_validation.PostgresRestoreService"
            ) as restore_cls:
                restore_cls.return_value.restore.return_value = True
                restore_cls.return_value.stop.return_value = True

                report = service.validate()

            self.assertEqual(report["state"], "passed")
            self.assertTrue(report["checks"]["temporary_restore"]["passed"])
            self.assertTrue(report["checks"]["table_comparison"]["matches"])
            self.assertTrue(report["temporary_instance"]["stopped"])
            self.assertTrue(report["temporary_instance"]["cleaned_up"])
            restore_cls.assert_called_once()

    def test_validate_fails_when_row_count_differs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backup_dir = root / "backup"
            archive_dir = root / "archive"
            backup_dir.mkdir()
            archive_dir.mkdir()

            service = self._build_service(backup_dir, archive_dir)
            source_snapshot = {
                "state": "passed",
                "tables": [
                    {
                        "schema": "public",
                        "table": "orders",
                        "row_count": 10,
                        "checksum": "source",
                    }
                ],
            }
            restored_snapshot = {
                "state": "passed",
                "tables": [
                    {
                        "schema": "public",
                        "table": "orders",
                        "row_count": 9,
                        "checksum": "restored",
                    }
                ],
            }

            with patch.object(
                service,
                "_collect_table_signatures",
                side_effect=[source_snapshot, restored_snapshot],
            ), patch(
                "services.validation.postgres_restore_validation.PostgresRestoreService"
            ) as restore_cls:
                restore_cls.return_value.restore.return_value = True
                restore_cls.return_value.stop.return_value = True

                report = service.validate()

            comparison = report["checks"]["table_comparison"]

            self.assertEqual(report["state"], "failed")
            self.assertFalse(comparison["matches"])
            self.assertEqual(
                comparison["row_count_mismatches"],
                [{"table": "public.orders", "source": 10, "restored": 9}],
            )
            self.assertEqual(
                comparison["checksum_mismatches"],
                [{"table": "public.orders", "source": "source", "restored": "restored"}],
            )


if __name__ == "__main__":
    unittest.main()
