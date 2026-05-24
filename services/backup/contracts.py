from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable


METADATA_SCHEMA_VERSION = "1.0"
VALIDATION_SCHEMA_VERSION = "1.0"
DEFAULT_RECOVERY_STATUS = "not_tested"
WAL_FILENAME_PATTERN = re.compile(r"^[0-9A-F]{24}$")


@dataclass
class ArtifactChecksum:
    path: str
    algorithm: str
    checksum_sha256: str
    size_bytes: int


@dataclass
class ValidationSummary:
    state: str
    report_path: str
    metadata_schema_valid: bool
    manifest_present: bool
    checkpoints_present: bool
    recovery_status: str


@dataclass
class RecoveryStatus:
    state: str = DEFAULT_RECOVERY_STATUS
    checked_at: str | None = None
    details: str | None = None


@dataclass
class ContractValidationReport:
    schema_version: str
    backup_id: str
    engine: str
    backup_type: str
    status: str
    validated_at: str
    metadata_schema: Dict[str, Any]
    artifact_validation: Dict[str, Any]
    artifacts: Dict[str, Any]
    manifest: Dict[str, Any]
    checkpoints: Dict[str, Any]
    markers: Dict[str, Any]
    recovery_status: Dict[str, Any]
    restore_validation: Dict[str, Any] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)


class BackupContractManager:
    def __init__(self, logger, messenger=None):
        self._logger = logger
        self._messenger = messenger

    def validate_backup_document(
        self,
        metadata_doc: Dict[str, Any],
        output_dir: Path | str,
    ) -> Dict[str, Any]:
        """
        Re-run artifact validation for an existing backup metadata document.

        This is intentionally narrower than persist_backup_documents(): it does
        not rewrite metadata.json or change backup completion state. It only
        refreshes current artifact status and rewrites validation.json.
        """
        output_dir = Path(output_dir)
        validation_doc = self._build_validation_document(
            self._refresh_metadata_for_validation(metadata_doc, output_dir),
            output_dir,
        )

        for key in ("metadata_path", "validation_path"):
            if key in validation_doc["artifacts"]:
                validation_doc["artifacts"][key]["exists"] = True
                validation_doc["artifacts"][key]["is_file"] = True

        self._write_json(output_dir / "validation.json", validation_doc)
        return validation_doc

    def persist_backup_documents(
        self,
        metadata: Dict[str, Any],
        output_dir: Path | str,
        backup_succeeded: bool,
    ) -> Dict[str, Any]:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        metadata_doc = self._build_metadata_document(metadata, output_dir)
        validation_doc = self._build_validation_document(metadata_doc, output_dir)
        validation_doc["status"] = (
            "passed" if backup_succeeded and validation_doc["status"] == "passed" else "failed"
        )

        validation_summary = ValidationSummary(
            state=validation_doc["status"],
            report_path=str(output_dir / "validation.json"),
            metadata_schema_valid=validation_doc["metadata_schema"]["valid"],
            manifest_present=validation_doc["manifest"]["exists"],
            checkpoints_present=validation_doc["checkpoints"]["exists"],
            recovery_status=validation_doc["recovery_status"]["state"],
        )

        metadata_doc["validation_status"] = asdict(validation_summary)
        metadata_doc["status"] = (
            "completed" if backup_succeeded and validation_summary.state == "passed" else "failed"
        )
        metadata_doc["metadata_path"] = str(output_dir / "metadata.json")
        metadata_doc["validation_path"] = str(output_dir / "validation.json")
        for key in ("metadata_path", "validation_path"):
            if key in validation_doc["artifacts"]:
                validation_doc["artifacts"][key]["exists"] = True
                validation_doc["artifacts"][key]["is_file"] = True

        self._write_json(output_dir / "metadata.json", metadata_doc)
        self._write_json(output_dir / "validation.json", validation_doc)

        return metadata_doc

    def _build_metadata_document(self, metadata: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
        normalized = deepcopy(metadata)

        backup_type = normalized.get("backup_type") or normalized.get("type") or "unknown"
        engine = normalized.get("engine") or normalized.get("database_type") or "unknown"
        backup_id = normalized.get("backup_id") or normalized.get("id") or output_dir.name

        normalized["schema_version"] = METADATA_SCHEMA_VERSION
        normalized["backup_id"] = backup_id
        normalized["id"] = backup_id
        normalized["backup_type"] = backup_type
        normalized["type"] = backup_type
        normalized["engine"] = engine
        normalized["database_type"] = engine
        normalized["backup_location"] = str(output_dir)
        normalized.setdefault("storage", "local")
        normalized.setdefault("compress", False)
        normalized.setdefault("compress_format", None)
        normalized.setdefault("backup_size_bytes", self._calculate_dir_size(output_dir))
        normalized.setdefault("backup_manifest_path", "")
        normalized.setdefault("backup_checkpoints_path", "")
        normalized.setdefault("binlog_info", "")
        normalized.setdefault("statistics", {
            "total_tables": 0,
            "total_rows_processed": 0,
            "total_size_bytes": normalized.get("backup_size_bytes", 0),
        })

        artifact_paths = self._build_artifact_paths(normalized, output_dir)
        normalized["artifact_paths"] = artifact_paths
        normalized["metadata_path"] = artifact_paths["metadata_path"]
        normalized["validation_path"] = artifact_paths["validation_path"]
        normalized["backup_manifest_path"] = artifact_paths["manifest_path"]
        normalized["backup_checkpoints_path"] = artifact_paths["checkpoints_path"]
        normalized["binlog_info_path"] = artifact_paths["binlog_info_path"]
        normalized["base_tar_path"] = artifact_paths["base_tar_path"]
        normalized["wal_tar_path"] = artifact_paths["wal_tar_path"]
        normalized["base_backup_reference_path"] = artifact_paths["base_backup_reference_path"]

        lsn_markers = self._build_lsn_markers(normalized)
        binlog_markers = self._build_binlog_markers(normalized)
        checksums = self._build_checksums(artifact_paths, output_dir, engine, backup_type)

        normalized["lsn_markers"] = lsn_markers
        normalized["binlog_markers"] = binlog_markers
        normalized["checksums"] = checksums
        normalized["manifest"] = self._build_artifact_status(artifact_paths["manifest_path"])
        normalized["checkpoints"] = self._build_artifact_status(artifact_paths["checkpoints_path"])
        normalized["validation_status"] = {
            "state": "pending",
            "report_path": artifact_paths["validation_path"],
            "metadata_schema_valid": False,
            "manifest_present": normalized["manifest"]["exists"],
            "checkpoints_present": normalized["checkpoints"]["exists"],
            "recovery_status": DEFAULT_RECOVERY_STATUS,
        }
        normalized.setdefault("restore_validation_required", False)
        normalized.setdefault("restore_validation", {})

        return normalized

    def _refresh_metadata_for_validation(
        self,
        metadata_doc: Dict[str, Any],
        output_dir: Path,
    ) -> Dict[str, Any]:
        refreshed = deepcopy(metadata_doc)
        artifact_paths = refreshed.get("artifact_paths")
        if isinstance(artifact_paths, dict):
            artifact_paths["backup_location"] = str(output_dir)
            artifact_paths["metadata_path"] = str(output_dir / "metadata.json")
            artifact_paths["validation_path"] = str(output_dir / "validation.json")

        refreshed["manifest"] = self._build_artifact_status(
            artifact_paths.get("manifest_path", "") if isinstance(artifact_paths, dict) else ""
        )
        refreshed["checkpoints"] = self._build_artifact_status(
            artifact_paths.get("checkpoints_path", "") if isinstance(artifact_paths, dict) else ""
        )

        if isinstance(artifact_paths, dict):
            old_checksums = refreshed.get("checksums", {})
            wal_checksums = {
                key: value
                for key, value in old_checksums.items()
                if isinstance(key, str) and key.startswith("wal::")
            } if isinstance(old_checksums, dict) else {}
            refreshed["checksums"] = self._build_checksums(
                artifact_paths,
                output_dir,
                refreshed.get("engine"),
                refreshed.get("backup_type"),
            )
            for key, value in wal_checksums.items():
                refreshed["checksums"].setdefault(key, value)

        return refreshed

    def _build_validation_document(
        self,
        metadata_doc: Dict[str, Any],
        output_dir: Path,
    ) -> Dict[str, Any]:
        metadata_errors = self.validate_metadata_document(metadata_doc)
        warnings = self._build_warnings(metadata_doc)
        artifact_report = self._build_artifact_report(metadata_doc, output_dir)
        missing_required = sorted(
            key
            for key, artifact in artifact_report.items()
            if artifact.get("required") and not artifact.get("exists")
        )
        recovery_status = self._build_recovery_status(metadata_doc)
        restore_validation_required = bool(metadata_doc.get("restore_validation_required"))
        recovery_valid = (
            not restore_validation_required
            or recovery_status.get("state") == "passed"
        )
        status = (
            "passed"
            if not metadata_errors and not missing_required and recovery_valid
            else "failed"
        )

        report = ContractValidationReport(
            schema_version=VALIDATION_SCHEMA_VERSION,
            backup_id=metadata_doc["id"],
            engine=metadata_doc["engine"],
            backup_type=metadata_doc["backup_type"],
            status=status,
            validated_at=datetime.now(timezone.utc).isoformat(),
            metadata_schema={
                "version": METADATA_SCHEMA_VERSION,
                "valid": not metadata_errors,
                "errors": metadata_errors,
            },
            artifact_validation={
                "valid": not missing_required,
                "missing_required": missing_required,
            },
            artifacts=artifact_report,
            manifest=metadata_doc["manifest"],
            checkpoints=metadata_doc["checkpoints"],
            markers={
                "lsn": metadata_doc["lsn_markers"],
                "binlog": metadata_doc["binlog_markers"],
            },
            recovery_status=recovery_status,
            restore_validation=metadata_doc.get("restore_validation") or {},
            warnings=warnings,
        )

        return asdict(report)

    @staticmethod
    def _build_recovery_status(metadata_doc: Dict[str, Any]) -> Dict[str, Any]:
        restore_validation = metadata_doc.get("restore_validation")
        restore_validation_required = bool(metadata_doc.get("restore_validation_required"))

        if isinstance(restore_validation, dict) and restore_validation:
            state = restore_validation.get("state") or DEFAULT_RECOVERY_STATUS
            details = restore_validation.get("error")
            if not details:
                comparison = restore_validation.get("checks", {}).get("table_comparison", {})
                if comparison:
                    details = (
                        "matched_tables="
                        f"{comparison.get('matched_tables', 0)}, "
                        "row_count_mismatches="
                        f"{len(comparison.get('row_count_mismatches', []))}, "
                        "checksum_mismatches="
                        f"{len(comparison.get('checksum_mismatches', []))}"
                    )

            return {
                "state": state,
                "checked_at": restore_validation.get("completed_at"),
                "details": details,
                "required": restore_validation_required,
            }

        recovery_status = asdict(RecoveryStatus())
        recovery_status["required"] = restore_validation_required
        return recovery_status

    def validate_metadata_document(self, metadata_doc: Dict[str, Any]) -> list[str]:
        errors: list[str] = []
        required_top_level = (
            "schema_version",
            "id",
            "backup_type",
            "engine",
            "database_name",
            "status",
            "timestamp_start",
            "artifact_paths",
            "lsn_markers",
            "binlog_markers",
            "checksums",
            "manifest",
            "checkpoints",
            "validation_status",
        )

        for key in required_top_level:
            if key not in metadata_doc:
                errors.append(f"Missing required key: {key}")

        if not isinstance(metadata_doc.get("artifact_paths"), dict):
            errors.append("artifact_paths must be a dictionary")
        else:
            required_artifacts = (
                "backup_location",
                "metadata_path",
                "validation_path",
                "manifest_path",
                "checkpoints_path",
                "archive_path",
                "binlog_info_path",
                "base_tar_path",
                "wal_tar_path",
                "base_backup_reference_path",
            )
            for key in required_artifacts:
                if key not in metadata_doc["artifact_paths"]:
                    errors.append(f"artifact_paths missing key: {key}")

        if not isinstance(metadata_doc.get("lsn_markers"), dict):
            errors.append("lsn_markers must be a dictionary")

        if not isinstance(metadata_doc.get("binlog_markers"), dict):
            errors.append("binlog_markers must be a dictionary")

        if not isinstance(metadata_doc.get("checksums"), dict):
            errors.append("checksums must be a dictionary")

        if not isinstance(metadata_doc.get("validation_status"), dict):
            errors.append("validation_status must be a dictionary")

        if "statistics" in metadata_doc and not isinstance(metadata_doc["statistics"], dict):
            errors.append("statistics must be a dictionary")

        return errors

    @staticmethod
    def _calculate_dir_size(path: Path) -> int:
        return sum(file_path.stat().st_size for file_path in path.rglob("*") if file_path.is_file())

    def _build_artifact_paths(self, metadata: Dict[str, Any], output_dir: Path) -> Dict[str, str]:
        manifest_path = metadata.get("backup_manifest_path") or self._infer_file(output_dir, "backup_manifest")
        checkpoints_path = (
            metadata.get("backup_checkpoints_path")
            or self._infer_file(output_dir, "xtrabackup_checkpoints")
        )
        binlog_info_path = metadata.get("binlog_info_path") or self._infer_file(output_dir, "xtrabackup_binlog_info")
        xtrabackup_info_path = metadata.get("xtrabackup_info_path") or self._infer_file(output_dir, "xtrabackup_info")
        ibdata1_path = metadata.get("ibdata1_path") or self._infer_file(output_dir, "ibdata1")
        mysql_directory_path = metadata.get("mysql_directory_path") or self._infer_directory(output_dir, "mysql")
        archive_path = metadata.get("archive_path") or ""
        base_tar_path = metadata.get("base_tar_path") or self._infer_file(output_dir, "base.tar.gz")
        wal_tar_path = metadata.get("wal_tar_path") or self._infer_file(output_dir, "pg_wal.tar.gz")
        base_backup_reference_path = (
            metadata.get("base_backup_reference_path")
            or self._infer_file(output_dir, "base_backup_id.txt")
        )

        return {
            "backup_location": str(output_dir),
            "metadata_path": str(output_dir / "metadata.json"),
            "validation_path": str(output_dir / "validation.json"),
            "archive_path": archive_path,
            "manifest_path": manifest_path,
            "checkpoints_path": checkpoints_path,
            "binlog_info_path": binlog_info_path,
            "xtrabackup_info_path": xtrabackup_info_path,
            "ibdata1_path": ibdata1_path,
            "mysql_directory_path": mysql_directory_path,
            "base_tar_path": base_tar_path,
            "wal_tar_path": wal_tar_path,
            "base_backup_reference_path": base_backup_reference_path,
        }

    @staticmethod
    def _infer_file(output_dir: Path, filename: str) -> str:
        path = output_dir / filename
        return str(path) if path.exists() else ""

    @staticmethod
    def _infer_directory(output_dir: Path, dirname: str) -> str:
        path = output_dir / dirname
        return str(path) if path.is_dir() else ""

    @staticmethod
    def _build_lsn_markers(metadata: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "start_lsn": metadata.get("start_lsn"),
            "end_lsn": metadata.get("end_lsn"),
            "current_lsn": metadata.get("current_lsn"),
            "current_wal_file": metadata.get("current_wal_file"),
            "previous_wal_file": metadata.get("previous_wal_file"),
            "last_backup_wal_file": metadata.get("last_backup_wal_file"),
        }

    def _build_binlog_markers(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        binlog_info = metadata.get("binlog_info", "")
        binlog_file = metadata.get("binlog_file")
        binlog_position = metadata.get("binlog_position")

        if binlog_info and (binlog_file is None or binlog_position is None):
            parsed_file, parsed_position = self._parse_binlog_info(binlog_info)
            binlog_file = binlog_file or parsed_file
            binlog_position = binlog_position if binlog_position is not None else parsed_position

        return {
            "binlog_info": binlog_info,
            "binlog_file": binlog_file,
            "binlog_position": binlog_position,
        }

    @staticmethod
    def _parse_binlog_info(binlog_info: str) -> tuple[str | None, int | None]:
        tokens = re.split(r"\s+", binlog_info.strip())
        if len(tokens) < 2:
            return None, None

        try:
            return tokens[0], int(tokens[1])
        except ValueError:
            return tokens[0], None

    def _build_checksums(
        self,
        artifact_paths: Dict[str, str],
        output_dir: Path,
        engine: str,
        backup_type: str,
    ) -> Dict[str, Dict[str, Any]]:
        checksum_map: Dict[str, Dict[str, Any]] = {}

        tracked_items = {
            key: path
            for key, path in artifact_paths.items()
            if key not in {"metadata_path", "validation_path"} and path
        }

        if engine == "postgresql" and backup_type in {"differential", "incremental"}:
            for wal_file in self._iter_postgres_wal_files(output_dir):
                tracked_items[f"wal::{wal_file.name}"] = str(wal_file)

        for key, path in tracked_items.items():
            file_path = Path(path)
            if not file_path.exists() or not file_path.is_file():
                continue
            checksum_map[key] = asdict(self._calculate_checksum(file_path))

        return checksum_map

    @staticmethod
    def _iter_postgres_wal_files(output_dir: Path) -> Iterable[Path]:
        for item in sorted(output_dir.iterdir()):
            if item.is_file() and WAL_FILENAME_PATTERN.match(item.name):
                yield item

    @staticmethod
    def _calculate_checksum(file_path: Path) -> ArtifactChecksum:
        hasher = hashlib.sha256()
        with file_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                hasher.update(chunk)

        return ArtifactChecksum(
            path=str(file_path),
            algorithm="sha256",
            checksum_sha256=hasher.hexdigest(),
            size_bytes=file_path.stat().st_size,
        )

    @staticmethod
    def _build_artifact_status(path: str) -> Dict[str, Any]:
        file_path = Path(path) if path else None
        exists = bool(file_path and file_path.exists())

        return {
            "path": path,
            "exists": exists,
        }

    def _build_artifact_report(self, metadata_doc: Dict[str, Any], output_dir: Path) -> Dict[str, Any]:
        artifact_paths = metadata_doc["artifact_paths"]
        checksums = metadata_doc["checksums"]
        required_keys = self._required_artifact_keys(metadata_doc)

        report: Dict[str, Any] = {}
        for key, path in artifact_paths.items():
            file_path = Path(path) if path else None
            exists = bool(file_path and file_path.exists())
            report[key] = {
                "path": path,
                "required": key in required_keys,
                "exists": exists,
                "is_file": bool(file_path and file_path.is_file()) if file_path else False,
                "is_dir": bool(file_path and file_path.is_dir()) if file_path else False,
                "checksum_sha256": checksums.get(key, {}).get("checksum_sha256"),
            }

        for key, checksum in checksums.items():
            if not isinstance(key, str) or not key.startswith("wal::"):
                continue
            wal_name = key.split("::", 1)[1]
            path = checksum.get("path") or str(output_dir / wal_name)
            file_path = Path(path)
            report[key] = {
                "path": path,
                "required": True,
                "exists": file_path.exists(),
                "is_file": file_path.is_file(),
                "is_dir": file_path.is_dir(),
                "checksum_sha256": checksum.get("checksum_sha256"),
            }

        for wal_item in metadata_doc.get("wal_files", []) or []:
            if not isinstance(wal_item, dict):
                continue
            wal_name = wal_item.get("filename")
            if not wal_name:
                continue
            key = f"wal::{wal_name}"
            if key in report:
                continue
            path = wal_item.get("backup_path") or str(output_dir / wal_name)
            file_path = Path(path)
            report[key] = {
                "path": path,
                "required": True,
                "exists": file_path.exists(),
                "is_file": file_path.is_file(),
                "is_dir": file_path.is_dir(),
                "checksum_sha256": wal_item.get("checksum_sha256"),
            }

        for wal_file in self._iter_postgres_wal_files(output_dir):
            report[f"wal::{wal_file.name}"] = {
                "path": str(wal_file),
                "required": True,
                "exists": True,
                "is_file": True,
                "is_dir": False,
                "checksum_sha256": checksums.get(f"wal::{wal_file.name}", {}).get("checksum_sha256"),
            }

        return report

    @staticmethod
    def _required_artifact_keys(metadata_doc: Dict[str, Any]) -> set[str]:
        required = {"backup_location"}
        engine = metadata_doc.get("engine")
        backup_type = metadata_doc.get("backup_type")

        if engine == "postgresql" and backup_type == "full":
            required.update({"base_tar_path", "wal_tar_path"})
        if engine == "postgresql" and backup_type in {"differential", "incremental"}:
            required.add("base_backup_reference_path")
        if engine == "mysql" and backup_type in {"full", "differential"}:
            required.add("checkpoints_path")

        explicit_required = (
            metadata_doc.get("required_artifacts")
            or metadata_doc.get("required_artifact_keys")
        )
        if isinstance(explicit_required, dict):
            required.update(
                key for key, is_required in explicit_required.items() if is_required
            )
        elif isinstance(explicit_required, (list, tuple, set)):
            required.update(str(key) for key in explicit_required)

        return required

    @staticmethod
    def _build_warnings(metadata_doc: Dict[str, Any]) -> list[str]:
        warnings: list[str] = []

        if not metadata_doc["manifest"]["exists"]:
            warnings.append("Backup manifest is absent.")

        if metadata_doc.get("engine") == "mysql" and not metadata_doc["checkpoints"]["exists"]:
            warnings.append("xtrabackup_checkpoints is absent.")

        if metadata_doc.get("engine") == "postgresql":
            lsn_markers = metadata_doc.get("lsn_markers", {})
            if not lsn_markers.get("current_wal_file") and not lsn_markers.get("last_backup_wal_file"):
                warnings.append("PostgreSQL WAL markers are incomplete.")

        if metadata_doc.get("engine") == "mysql":
            binlog_markers = metadata_doc.get("binlog_markers", {})
            if not binlog_markers.get("binlog_info"):
                warnings.append("MySQL binlog markers are absent.")

        return warnings

    @staticmethod
    def _write_json(path: Path, payload: Dict[str, Any]) -> None:
        with path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2, ensure_ascii=False, default=str)
            handle.write("\n")
