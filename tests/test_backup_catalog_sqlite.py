from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from services.backup.catalog import BackupCatalog


class BackupCatalogSQLiteTests(unittest.TestCase):
    def test_imports_legacy_json_and_filters_by_database(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            db_path = temp_path / "catalog.db"
            legacy_path = temp_path / "backup_catalog.json"
            legacy_payload = {
                "backups": [
                    {
                        "id": "full_pg_app_001",
                        "type": "full",
                        "backup_type": "full",
                        "database_name": "appdb",
                        "database_type": "postgresql",
                        "engine": "postgresql",
                        "status": "completed",
                        "timestamp_start": "2026-04-20T10:00:00+00:00",
                    },
                    {
                        "id": "full_mysql_001",
                        "type": "full",
                        "backup_type": "full",
                        "database_name": "otherdb",
                        "database_type": "mysql",
                        "engine": "mysql",
                        "status": "completed",
                        "timestamp_start": "2026-04-20T11:00:00+00:00",
                    },
                ]
            }
            legacy_path.write_text(json.dumps(legacy_payload), encoding="utf-8")

            catalog = BackupCatalog(path=str(db_path), legacy_json_path=str(legacy_path))

            self.assertEqual(catalog.get_last_full_backup(database_name="appdb")["id"], "full_pg_app_001")
            self.assertEqual(catalog.get_last_full_backup(database_name="otherdb")["id"], "full_mysql_001")

    def test_builds_backup_chain_from_parent_and_base_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "catalog.db"
            catalog = BackupCatalog(path=str(db_path))

            full_backup = {
                "id": "full_pg_app_001",
                "type": "full",
                "backup_type": "full",
                "database_name": "appdb",
                "database_type": "postgresql",
                "engine": "postgresql",
                "status": "completed",
                "timestamp_start": "2026-04-20T10:00:00+00:00",
                "timestamp_end": "2026-04-20T10:05:00+00:00",
                "duration_seconds": 300.0,
            }
            differential_backup = {
                "id": "differential_pg_app_002",
                "type": "differential",
                "backup_type": "differential",
                "database_name": "appdb",
                "database_type": "postgresql",
                "engine": "postgresql",
                "status": "completed",
                "timestamp_start": "2026-04-20T11:00:00+00:00",
                "timestamp_end": "2026-04-20T11:01:00+00:00",
                "duration_seconds": 60.0,
                "parent_backup_id": "full_pg_app_001",
                "base_backup_id": "full_pg_app_001",
            }

            catalog.add_backup(full_backup)
            catalog.add_backup(differential_backup)

            chain = catalog.get_backup_chain("differential_pg_app_002")
            chain_ids = [item["id"] for item in chain]

            self.assertEqual(chain_ids, ["full_pg_app_001", "differential_pg_app_002"])
            self.assertEqual(catalog.get_last_successful_backup(database_name="appdb")["id"], "differential_pg_app_002")

    def test_finds_latest_pitr_base_backup_before_target_time(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir) / "catalog.db"
            catalog = BackupCatalog(path=str(db_path))

            backups = [
                {
                    "id": "full_pg_app_old",
                    "type": "full",
                    "backup_type": "full",
                    "database_name": "appdb",
                    "database_type": "postgresql",
                    "engine": "postgresql",
                    "status": "completed",
                    "timestamp_start": "2026-05-04T09:55:00+00:00",
                    "timestamp_end": "2026-05-04T10:00:00+00:00",
                },
                {
                    "id": "full_pg_app_selected",
                    "type": "full",
                    "backup_type": "full",
                    "database_name": "appdb",
                    "database_type": "postgresql",
                    "engine": "postgresql",
                    "status": "completed",
                    "timestamp_start": "2026-05-04T11:55:00+00:00",
                    "timestamp_end": "2026-05-04T12:00:00+00:00",
                },
                {
                    "id": "full_pg_app_future",
                    "type": "full",
                    "backup_type": "full",
                    "database_name": "appdb",
                    "database_type": "postgresql",
                    "engine": "postgresql",
                    "status": "completed",
                    "timestamp_start": "2026-05-04T12:45:00+00:00",
                    "timestamp_end": "2026-05-04T12:50:00+00:00",
                },
            ]
            for backup in backups:
                catalog.add_backup(backup)

            selected = catalog.find_pitr_base_backup(
                database_name="appdb",
                target_time=datetime(2026, 5, 4, 12, 30, tzinfo=timezone.utc),
            )

            self.assertEqual(selected["id"], "full_pg_app_selected")


if __name__ == "__main__":
    unittest.main()
