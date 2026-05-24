from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from services.backup.contracts import BackupContractManager


class _NullLogger:
    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str, exc_info: bool = False) -> None:
        pass


class BackupArtifactValidationService:
    def __init__(self, logger=None, messenger=None) -> None:
        self._logger = logger or _NullLogger()
        self._messenger = messenger
        self._contract_manager = BackupContractManager(self._logger, messenger)

    def validate_backup(self, backup_path: str | Path | None) -> dict[str, Any]:
        if backup_path is None:
            return self._fail("Path is required. Use: validate backup -path <backup_directory>")

        backup_dir = Path(backup_path).expanduser()
        if not backup_dir.exists():
            return self._fail(f"Invalid path: {backup_dir}")
        if not backup_dir.is_dir():
            return self._fail(f"Path is not a backup directory: {backup_dir}")

        metadata_path = backup_dir / "metadata.json"
        if not metadata_path.is_file():
            return self._fail(f"Missing metadata.json: {metadata_path}")

        try:
            with metadata_path.open("r", encoding="utf-8") as handle:
                metadata_doc = json.load(handle)
        except json.JSONDecodeError as error:
            return self._fail(f"Invalid metadata.json: {error}")

        try:
            validation_doc = self._contract_manager.validate_backup_document(
                metadata_doc=metadata_doc,
                output_dir=backup_dir,
            )
        except Exception as error:
            return self._fail(f"Validation could not be completed: {error}")

        missing_required = validation_doc.get("artifact_validation", {}).get(
            "missing_required",
            [],
        )
        if validation_doc.get("status") == "passed":
            print("Validation passed")
        else:
            print("Validation failed")
            self._print_missing_required(missing_required)

        return validation_doc

    def _fail(self, message: str) -> dict[str, Any]:
        print("Validation failed")
        print(message)
        return {
            "status": "failed",
            "error": message,
            "artifact_validation": {
                "valid": False,
                "missing_required": [],
            },
        }

    @staticmethod
    def _print_missing_required(missing_required: list[str]) -> None:
        if not missing_required:
            return

        print("Missing required artifacts:")
        for artifact in missing_required:
            print(f"  - {artifact}")
