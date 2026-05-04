from __future__ import annotations

import tempfile
import unittest
from datetime import timezone
from pathlib import Path

from services.backup.catalog import BackupCatalog
from services.restore.pitr import PostgresPitrPlanner, parse_recovery_target_time


class PostgresPitrPlannerTests(unittest.TestCase):
    def test_planner_selects_base_backup_and_formats_target_time(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backup_dir = root / "full_pg_app"
            backup_dir.mkdir()
            catalog = BackupCatalog(path=str(root / "catalog.db"))
            catalog.add_backup(
                {
                    "id": "full_pg_app",
                    "type": "full",
                    "backup_type": "full",
                    "database_name": "appdb",
                    "database_type": "postgresql",
                    "engine": "postgresql",
                    "status": "completed",
                    "timestamp_start": "2026-05-04T11:55:00+00:00",
                    "timestamp_end": "2026-05-04T12:00:00+00:00",
                    "backup_location": str(backup_dir),
                }
            )

            plan = PostgresPitrPlanner(
                database_name="appdb",
                target_time="2026-05-04 12:30:00+00:00",
                catalog=catalog,
            ).build_plan()

            self.assertEqual(plan.backup["id"], "full_pg_app")
            self.assertEqual(plan.backup_path, backup_dir.resolve())
            self.assertEqual(plan.recovery_target_time, "2026-05-04 12:30:00+00:00")

    def test_parse_recovery_target_time_accepts_naive_timestamp(self) -> None:
        target_time = parse_recovery_target_time("2026-05-04 12:30:00")

        self.assertEqual(target_time.tzinfo, timezone.utc)
        self.assertEqual(target_time.isoformat(), "2026-05-04T12:30:00+00:00")

    def test_parse_recovery_target_time_normalizes_offsets_to_utc(self) -> None:
        target_time = parse_recovery_target_time("2026-05-04T12:30:00+03:00")

        self.assertEqual(target_time.tzinfo, timezone.utc)
        self.assertEqual(target_time.isoformat(), "2026-05-04T09:30:00+00:00")


if __name__ == "__main__":
    unittest.main()
