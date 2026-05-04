from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.backup.catalog import BackupCatalog


PITR_TIMEZONE_POLICY = (
    "PITR target times are normalized to UTC; timestamps without timezone are treated as UTC."
)


@dataclass(frozen=True)
class PitrRestorePlan:
    backup: dict[str, Any]
    backup_path: Path
    target_time: datetime
    recovery_target_time: str


def parse_recovery_target_time(value: str) -> datetime:
    if not value or not value.strip():
        raise ValueError("PITR target time cannot be empty.")

    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        target_time = datetime.fromisoformat(normalized)
    except ValueError as error:
        raise ValueError(
            "Invalid --target-time value. Use an ISO-like timestamp, for example "
            "'2026-05-04 12:30:00' for UTC or '2026-05-04T12:30:00+03:00'."
        ) from error

    if target_time.tzinfo is None:
        return target_time.replace(tzinfo=timezone.utc)

    return target_time.astimezone(timezone.utc)


def format_recovery_target_time(target_time: datetime) -> str:
    timespec = "microseconds" if target_time.microsecond else "seconds"
    return target_time.isoformat(sep=" ", timespec=timespec)


class PostgresPitrPlanner:
    def __init__(
        self,
        database_name: str,
        target_time: str,
        catalog_path: str = "backup_catalog.db",
        catalog: BackupCatalog | None = None,
        messenger=None,
    ) -> None:
        self._database_name = database_name
        self._target_time = target_time
        self._catalog = catalog or BackupCatalog(path=catalog_path)
        self._messenger = messenger

    def build_plan(self) -> PitrRestorePlan:
        target_time = parse_recovery_target_time(self._target_time)
        backup = self._catalog.find_pitr_base_backup(
            database_name=self._database_name,
            target_time=target_time,
        )
        if not backup:
            raise ValueError(
                "No completed PostgreSQL full backup found at or before "
                f"target time {format_recovery_target_time(target_time)} "
                f"for database {self._database_name}."
            )

        backup_path = self._resolve_backup_path(backup)
        recovery_target_time = format_recovery_target_time(target_time)

        self._message_info(
            "Selected PITR base backup "
            f"{backup.get('id') or backup.get('backup_id')} at {backup_path}"
        )
        self._message_info(PITR_TIMEZONE_POLICY)
        self._message_info(f"Recovery target time: {recovery_target_time}")

        return PitrRestorePlan(
            backup=backup,
            backup_path=backup_path,
            target_time=target_time,
            recovery_target_time=recovery_target_time,
        )

    def _resolve_backup_path(self, backup: dict[str, Any]) -> Path:
        candidates: list[Path] = []
        self._append_candidate(candidates, backup.get("backup_location"))
        self._append_candidate(candidates, backup.get("archive_path"))

        artifact_paths = backup.get("artifact_paths")
        if isinstance(artifact_paths, dict):
            self._append_candidate(candidates, artifact_paths.get("archive_path"))

        for candidate in candidates:
            if candidate.exists():
                return candidate.resolve()

        candidate_list = ", ".join(str(path) for path in candidates) or "(none)"
        raise FileNotFoundError(
            "Selected PITR base backup has no accessible backup directory or "
            f"single archive. Checked: {candidate_list}"
        )

    @staticmethod
    def _append_candidate(candidates: list[Path], value: Any) -> None:
        if isinstance(value, str) and value.strip():
            candidate = Path(value).expanduser()
            if candidate not in candidates:
                candidates.append(candidate)

    def _message_info(self, message: str) -> None:
        if self._messenger is not None:
            self._messenger.info(message)
