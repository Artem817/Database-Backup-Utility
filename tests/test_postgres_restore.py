from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from services.restore.postgres_restore import PostgresRestoreService


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


class PostgresRestoreServiceTests(unittest.TestCase):
    def _build_service(
        self,
        backup_dir: Path,
        data_dir: Path,
        archive_dir: Path,
        target_time: str | None = None,
    ) -> PostgresRestoreService:
        return PostgresRestoreService(
            backup_path=str(backup_dir),
            data_dir=str(data_dir),
            archive_dir=str(archive_dir),
            restore_port=55432,
            verify_database="postgres",
            verify_user="postgres",
            verify_query="SELECT 1;",
            target_time=target_time,
            logger=DummyLogger(),
            messenger=DummyMessenger(),
        )

    def test_write_restore_configuration_adds_restore_command_and_port(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backup_dir = root / "backup"
            data_dir = root / "data"
            archive_dir = root / "archive wal"
            backup_dir.mkdir()
            data_dir.mkdir()
            archive_dir.mkdir()
            (data_dir / "postgresql.auto.conf").write_text("shared_buffers = '128MB'\n", encoding="utf-8")

            service = self._build_service(backup_dir, data_dir, archive_dir)
            service._write_restore_configuration()
            service._create_recovery_signal()

            auto_conf = (data_dir / "postgresql.auto.conf").read_text(encoding="utf-8")

            self.assertIn("shared_buffers = '128MB'", auto_conf)
            self.assertIn("restore_command = 'cp \"", auto_conf)
            self.assertIn("/archive wal/%f\" \"%p\"'", auto_conf)
            self.assertIn("archive_mode = off", auto_conf)
            self.assertIn("port = 55432", auto_conf)
            self.assertIn("listen_addresses = '127.0.0.1'", auto_conf)
            self.assertTrue((data_dir / "recovery.signal").exists())

    def test_write_restore_configuration_adds_pitr_target_time(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backup_dir = root / "backup"
            data_dir = root / "data"
            archive_dir = root / "archive"
            backup_dir.mkdir()
            data_dir.mkdir()
            archive_dir.mkdir()

            service = self._build_service(
                backup_dir,
                data_dir,
                archive_dir,
                target_time="2026-05-04 12:30:00+03:00",
            )
            service._write_restore_configuration()

            auto_conf = (data_dir / "postgresql.auto.conf").read_text(encoding="utf-8")

            self.assertIn("recovery_target_time = '2026-05-04 09:30:00+00:00'", auto_conf)
            self.assertIn("recovery_target_action = 'promote'", auto_conf)
            self.assertIn("recovery_target_timeline = 'latest'", auto_conf)
            self.assertIn("restore_command = 'cp \"", auto_conf)

    def test_prepend_local_hba_rules_adds_managed_block_first(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backup_dir = root / "backup"
            data_dir = root / "data"
            archive_dir = root / "archive"
            backup_dir.mkdir()
            data_dir.mkdir()
            archive_dir.mkdir()
            (data_dir / "pg_hba.conf").write_text(
                "host all all 127.0.0.1/32 scram-sha-256\n",
                encoding="utf-8",
            )

            service = self._build_service(backup_dir, data_dir, archive_dir)
            service._prepend_local_hba_rules()

            pg_hba = (data_dir / "pg_hba.conf").read_text(encoding="utf-8")

            self.assertTrue(pg_hba.startswith(service.HBA_BLOCK_START))
            self.assertIn("local   all             all                                     trust", pg_hba)
            self.assertIn("host all all 127.0.0.1/32 scram-sha-256", pg_hba)

    def test_validate_backup_directory_rejects_tablespace_tar_archives(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backup_dir = root / "backup"
            data_dir = root / "data"
            archive_dir = root / "archive"
            backup_dir.mkdir()
            data_dir.mkdir()
            archive_dir.mkdir()

            (backup_dir / "base.tar.gz").write_bytes(b"base")
            (backup_dir / "pg_wal.tar.gz").write_bytes(b"wal")
            (backup_dir / "16384.tar.gz").write_bytes(b"tablespace")

            service = self._build_service(backup_dir, data_dir, archive_dir)

            with self.assertRaises(ValueError):
                service._validate_backup_directory(backup_dir)


if __name__ == "__main__":
    unittest.main()
