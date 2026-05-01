from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from services.backup.contracts import BackupContractManager


class DummyLogger:
    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str, exc_info: bool = False) -> None:
        pass


class BackupContractManagerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.manager = BackupContractManager(DummyLogger())
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def test_persist_backup_documents_writes_versioned_metadata_and_validation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backup_dir = Path(temp_dir) / "full_pg"
            backup_dir.mkdir()
            (backup_dir / "base.tar.gz").write_bytes(b"base-data")
            (backup_dir / "pg_wal.tar.gz").write_bytes(b"wal-data")

            metadata = {
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
            }

            result = self.manager.persist_backup_documents(
                metadata=metadata,
                output_dir=backup_dir,
                backup_succeeded=True,
            )

            metadata_doc = json.loads((backup_dir / "metadata.json").read_text(encoding="utf-8"))
            validation_doc = json.loads((backup_dir / "validation.json").read_text(encoding="utf-8"))

            self.assertEqual(result["schema_version"], "1.0")
            self.assertEqual(metadata_doc["schema_version"], "1.0")
            self.assertEqual(metadata_doc["backup_type"], "full")
            self.assertEqual(metadata_doc["engine"], "postgresql")
            self.assertEqual(metadata_doc["validation_status"]["state"], "passed")
            self.assertIn("base_tar_path", metadata_doc["checksums"])
            self.assertEqual(validation_doc["status"], "passed")
            self.assertTrue(validation_doc["metadata_schema"]["valid"])
            self.assertEqual(
                validation_doc["markers"]["lsn"]["current_wal_file"],
                "000000010000000000000003",
            )

    def test_validation_fails_when_required_artifact_is_missing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            backup_dir = Path(temp_dir) / "broken_pg"
            backup_dir.mkdir()
            (backup_dir / "base.tar.gz").write_bytes(b"base-data")

            metadata = {
                "id": "full_pg_002",
                "type": "full",
                "database_type": "postgresql",
                "database_name": "appdb",
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
            }

            result = self.manager.persist_backup_documents(
                metadata=metadata,
                output_dir=backup_dir,
                backup_succeeded=True,
            )
            validation_doc = json.loads((backup_dir / "validation.json").read_text(encoding="utf-8"))

            self.assertEqual(result["status"], "failed")
            self.assertEqual(result["validation_status"]["state"], "failed")
            self.assertEqual(validation_doc["status"], "failed")
            self.assertIn("wal_tar_path", validation_doc["artifact_validation"]["missing_required"])


if __name__ == "__main__":
    unittest.main()
