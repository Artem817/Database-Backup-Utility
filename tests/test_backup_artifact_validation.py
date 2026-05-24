from __future__ import annotations

import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

from services.backup.contracts import BackupContractManager
from services.backup.validation import BackupArtifactValidationService


class DummyLogger:
    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str, exc_info: bool = False) -> None:
        pass


class BackupArtifactValidationServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.contract_manager = BackupContractManager(DummyLogger())
        self.validation_service = BackupArtifactValidationService(logger=DummyLogger())
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def test_successful_validation_of_postgresql_full_backup(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backup_dir = self._create_postgres_full_backup(Path(temp_dir))

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                report = self.validation_service.validate_backup(backup_dir)

            validation_doc = json.loads(
                (backup_dir / "validation.json").read_text(encoding="utf-8")
            )
            self.assertEqual(report["status"], "passed")
            self.assertEqual(validation_doc["status"], "passed")
            self.assertIn("Validation passed", stdout.getvalue())

    def test_validation_fails_when_postgresql_wal_archive_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backup_dir = self._create_postgres_full_backup(Path(temp_dir))
            (backup_dir / "pg_wal.tar.gz").unlink()

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                report = self.validation_service.validate_backup(backup_dir)

            validation_doc = json.loads(
                (backup_dir / "validation.json").read_text(encoding="utf-8")
            )
            output = stdout.getvalue()

            self.assertEqual(report["status"], "failed")
            self.assertEqual(validation_doc["status"], "failed")
            self.assertIn(
                "wal_tar_path",
                validation_doc["artifact_validation"]["missing_required"],
            )
            self.assertIn("Validation failed", output)
            self.assertIn("wal_tar_path", output)

    def test_validation_fails_when_metadata_json_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backup_dir = Path(temp_dir) / "missing_metadata"
            backup_dir.mkdir()

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                report = self.validation_service.validate_backup(backup_dir)

            self.assertEqual(report["status"], "failed")
            self.assertIn("Missing metadata.json", report["error"])
            self.assertIn("Validation failed", stdout.getvalue())
            self.assertFalse((backup_dir / "validation.json").exists())

    def test_validation_fails_for_invalid_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backup_dir = Path(temp_dir) / "does_not_exist"

            stdout = io.StringIO()
            with redirect_stdout(stdout):
                report = self.validation_service.validate_backup(backup_dir)

            self.assertEqual(report["status"], "failed")
            self.assertIn("Invalid path", report["error"])
            self.assertIn("Validation failed", stdout.getvalue())

    def _create_postgres_full_backup(self, temp_dir: Path) -> Path:
        backup_dir = temp_dir / "full_pg"
        backup_dir.mkdir()
        (backup_dir / "base.tar.gz").write_bytes(b"base-data")
        (backup_dir / "pg_wal.tar.gz").write_bytes(b"wal-data")

        self.contract_manager.persist_backup_documents(
            metadata={
                "id": "full_pg_001",
                "type": "full",
                "database_type": "postgresql",
                "database_name": "appdb",
                "database_version": "16.2",
                "timestamp_start": self.timestamp,
                "timestamp_end": self.timestamp,
                "duration_seconds": 1.0,
                "status": "completed",
                "backup_location": str(backup_dir),
                "statistics": {
                    "total_tables": 0,
                    "total_rows_processed": 0,
                    "total_size_bytes": 0,
                },
                "current_lsn": "0/3000028",
                "current_wal_file": "000000010000000000000003",
            },
            output_dir=backup_dir,
            backup_succeeded=True,
        )
        return backup_dir


if __name__ == "__main__":
    unittest.main()
