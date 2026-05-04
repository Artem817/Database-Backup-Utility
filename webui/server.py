from __future__ import annotations

import argparse
import importlib.util
import json
import logging
import os
import threading
import traceback
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from custom_logging import BackupLogger
from services.backup.catalog import BackupCatalog


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATIC_ROOT = Path(__file__).resolve().parent / "static"
CATALOG_PATH = PROJECT_ROOT / "backup_catalog.db"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class JobRecord:
    id: str
    kind: str
    status: str
    title: str
    created_at: str
    updated_at: str
    payload: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] | None = None
    error: str | None = None
    logs: list[dict[str, str]] = field(default_factory=list)


class JobStore:
    def __init__(self) -> None:
        self._jobs: dict[str, JobRecord] = {}
        self._lock = threading.RLock()

    def create(self, kind: str, title: str, payload: dict[str, Any]) -> JobRecord:
        job_id = uuid.uuid4().hex[:12]
        now = utc_now()
        job = JobRecord(
            id=job_id,
            kind=kind,
            status="queued",
            title=title,
            created_at=now,
            updated_at=now,
            payload=self._redact_payload(payload),
        )
        with self._lock:
            self._jobs[job_id] = job
        return job

    def start(self, job_id: str) -> None:
        self.update(job_id, status="running")

    def append_log(self, job_id: str, level: str, message: str) -> None:
        with self._lock:
            job = self._jobs[job_id]
            job.logs.append(
                {
                    "time": utc_now(),
                    "level": level,
                    "message": str(message),
                }
            )
            job.logs = job.logs[-500:]
            job.updated_at = utc_now()

    def complete(self, job_id: str, result: dict[str, Any]) -> None:
        self.update(job_id, status="completed", result=result, error=None)

    def fail(self, job_id: str, error: str) -> None:
        self.update(job_id, status="failed", error=error)

    def update(self, job_id: str, **changes: Any) -> None:
        with self._lock:
            job = self._jobs[job_id]
            for key, value in changes.items():
                setattr(job, key, value)
            job.updated_at = utc_now()

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._lock:
            job = self._jobs.get(job_id)
            return asdict(job) if job else None

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            jobs = sorted(
                self._jobs.values(),
                key=lambda item: item.created_at,
                reverse=True,
            )
            return [asdict(job) for job in jobs[:50]]

    @staticmethod
    def _redact_payload(payload: dict[str, Any]) -> dict[str, Any]:
        redacted = dict(payload)
        if redacted.get("password"):
            redacted["password"] = "***"
        return redacted


JOB_STORE = JobStore()


class JobMessenger:
    def __init__(self, job_id: str) -> None:
        self._job_id = job_id

    def info(self, message: str) -> None:
        JOB_STORE.append_log(self._job_id, "info", message)

    def success(self, message: str) -> None:
        JOB_STORE.append_log(self._job_id, "success", message)

    def warning(self, message: str) -> None:
        JOB_STORE.append_log(self._job_id, "warning", message)

    def error(self, message: str) -> None:
        JOB_STORE.append_log(self._job_id, "error", message)

    def section_header(self, title: str) -> None:
        JOB_STORE.append_log(self._job_id, "section", title)


class JobLogHandler(logging.Handler):
    def __init__(self, job_id: str) -> None:
        super().__init__()
        self._job_id = job_id

    def emit(self, record: logging.LogRecord) -> None:
        try:
            JOB_STORE.append_log(
                self._job_id,
                record.levelname.lower(),
                self.format(record),
            )
        except Exception:
            pass


def build_job_logger(job_id: str, database_name: str) -> BackupLogger:
    safe_database = "".join(
        char if char.isalnum() or char in {"_", "-"} else "_"
        for char in database_name
    ) or "database"
    logger = BackupLogger(
        name=f"web_{safe_database}_{job_id}",
        log_file=f"web_backup_{safe_database}.log",
    )
    handler = JobLogHandler(job_id)
    handler.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
    logger.logger.addHandler(handler)
    return logger


class WebApiError(Exception):
    pass


def require_module(module_name: str, package_name: str) -> None:
    if importlib.util.find_spec(module_name) is not None:
        return
    raise WebApiError(
        f"Missing Python dependency: {package_name}. "
        "Start the Web UI from the project virtual environment "
        f"or install dependencies with: python3 -m pip install -r requirements.txt"
    )


def require_text(payload: dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise WebApiError(f"Missing required field: {key}")
    return value


def as_bool(payload: dict[str, Any], key: str, default: bool = False) -> bool:
    value = payload.get(key, default)
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in {"1", "true", "yes", "y", "t", "on"}
    return bool(value)


def as_int(payload: dict[str, Any], key: str, default: int) -> int:
    value = payload.get(key)
    if value in (None, ""):
        return default
    return int(value)


def start_thread(job_id: str, target, payload: dict[str, Any]) -> None:
    thread = threading.Thread(
        target=run_job_wrapper,
        args=(job_id, target, payload),
        daemon=True,
    )
    thread.start()


def run_job_wrapper(job_id: str, target, payload: dict[str, Any]) -> None:
    JOB_STORE.start(job_id)
    try:
        result = target(job_id, payload)
        JOB_STORE.complete(job_id, result)
    except Exception as error:
        JOB_STORE.append_log(job_id, "error", traceback.format_exc())
        JOB_STORE.fail(job_id, str(error))


def run_backup_job(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    engine = require_text(payload, "engine")
    if engine == "postgres":
        return run_postgres_backup(job_id, payload)
    if engine == "mysql":
        return run_mysql_backup(job_id, payload)
    raise WebApiError(f"Unsupported engine: {engine}")


def run_postgres_backup(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    require_module("psycopg2", "psycopg2-binary")

    from clients.postgres_client import PostgresClient

    database = require_text(payload, "database")
    output_path = require_text(payload, "output_path")
    logger = build_job_logger(job_id, database)
    messenger = JobMessenger(job_id)
    archive_dir = str(payload.get("archive_dir") or "").strip() or None

    client = PostgresClient(
        host=require_text(payload, "host"),
        port=as_int(payload, "port", 5432),
        database=database,
        user=require_text(payload, "user"),
        password=str(payload.get("password") or ""),
        use_pgpass=as_bool(payload, "use_pgpass", False),
        archive_path=archive_dir,
        configure_wal_archive=False,
        logger=logger,
        messenger=messenger,
    )

    try:
        if client.connect() is None:
            raise WebApiError("PostgreSQL connection failed")
        if not client.validate_connection():
            raise WebApiError("PostgreSQL connection validation failed")

        success = client.backup_full(
            outpath=output_path,
            single_archive=as_bool(payload, "single_archive", True),
            storage="local",
            validate_restore=as_bool(payload, "validate_restore", True),
            restore_validation_port=as_int(payload, "restore_validation_port", 0),
            restore_validation_keep_data=as_bool(
                payload,
                "restore_validation_keep_data",
                False,
            ),
            restore_validation_checksums=as_bool(
                payload,
                "restore_validation_checksums",
                True,
            ),
            pg_bin_dir=str(payload.get("pg_bin_dir") or "").strip() or None,
        )
        if not success:
            raise WebApiError("PostgreSQL backup finished with failed status")

        return {"success": True, "engine": "postgres", "database": database}
    finally:
        if client.is_connected:
            client.disconnect()


def run_mysql_backup(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    require_module("pymysql", "pymysql")

    from clients.mysql_client import MysqlClient

    database = require_text(payload, "database")
    output_path = require_text(payload, "output_path")
    logger = build_job_logger(job_id, database)
    messenger = JobMessenger(job_id)

    client = MysqlClient(
        host=str(payload.get("host") or "localhost"),
        port=as_int(payload, "port", 3306),
        database=database,
        user=str(payload.get("user") or ""),
        password=str(payload.get("password") or ""),
        login_path=str(payload.get("login_path") or "").strip() or None,
        socket=str(payload.get("socket") or "").strip() or None,
        logger=logger,
        messenger=messenger,
    )

    try:
        client.connect()
        if not client.validate_connection():
            raise WebApiError("MySQL connection validation failed")
        success = client.backup_full(
            outpath=output_path,
            single_archive=as_bool(payload, "single_archive", True),
            storage="local",
        )
        if not success:
            raise WebApiError("MySQL backup finished with failed status")

        return {"success": True, "engine": "mysql", "database": database}
    finally:
        if client.is_connected:
            client.disconnect()


def run_restore_validation_job(job_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    require_module("psycopg2", "psycopg2-binary")

    from services.validation.postgres_restore_validation import (
        PostgresRestoreValidationService,
    )

    database = require_text(payload, "database")
    logger = build_job_logger(job_id, database)
    messenger = JobMessenger(job_id)

    validator = PostgresRestoreValidationService(
        backup_path=require_text(payload, "backup_path"),
        archive_dir=require_text(payload, "archive_dir"),
        source_connection_params={
            "host": require_text(payload, "host"),
            "port": as_int(payload, "port", 5432),
            "user": require_text(payload, "user"),
            "password": str(payload.get("password") or ""),
            "database": database,
            "use_pgpass": as_bool(payload, "use_pgpass", False),
        },
        restore_port=as_int(payload, "restore_validation_port", 0),
        verify_database=database,
        verify_user=str(payload.get("verify_user") or payload.get("user") or "postgres"),
        pg_bin_dir=str(payload.get("pg_bin_dir") or "").strip() or None,
        checksum_tables=as_bool(payload, "restore_validation_checksums", True),
        keep_data_dir=as_bool(payload, "restore_validation_keep_data", False),
        logger=logger,
        messenger=messenger,
    )
    report = validator.validate()
    if report.get("state") != "passed":
        JOB_STORE.update(job_id, result=report)
        raise WebApiError("Restore validation failed")
    return report


class RequestHandler(BaseHTTPRequestHandler):
    server_version = "DBToolWebUI/1.0"

    def do_GET(self) -> None:
        self.handle_request("GET")

    def do_POST(self) -> None:
        self.handle_request("POST")

    def log_message(self, format: str, *args: Any) -> None:
        logging.getLogger("webui").info(format, *args)

    def handle_request(self, method: str) -> None:
        try:
            parsed = urlparse(self.path)
            path = parsed.path
            if method == "GET" and path == "/":
                self.serve_file(STATIC_ROOT / "index.html", "text/html; charset=utf-8")
                return
            if method == "GET" and path.startswith("/static/"):
                self.serve_static(path)
                return
            if method == "GET" and path == "/api/health":
                self.write_json({"status": "ok", "catalog": str(CATALOG_PATH)})
                return
            if method == "GET" and path == "/api/catalog":
                self.write_json({"backups": load_catalog_backups()})
                return
            if method == "GET" and path == "/api/backup/document":
                self.write_json(load_backup_document(parse_qs(parsed.query)))
                return
            if method == "GET" and path == "/api/jobs":
                self.write_json({"jobs": JOB_STORE.list()})
                return
            if method == "GET" and path.startswith("/api/jobs/"):
                job_id = path.rsplit("/", 1)[-1]
                job = JOB_STORE.get(job_id)
                if not job:
                    self.write_json({"error": "Job not found"}, status=404)
                    return
                self.write_json({"job": job})
                return
            if method == "POST" and path == "/api/backup/start":
                self.create_job("backup", "Backup", run_backup_job)
                return
            if method == "POST" and path == "/api/restore/validate":
                self.create_job(
                    "restore_validation",
                    "Restore validation",
                    run_restore_validation_job,
                )
                return

            self.write_json({"error": "Not found"}, status=404)
        except WebApiError as error:
            self.write_json({"error": str(error)}, status=400)
        except Exception as error:
            self.write_json(
                {"error": str(error), "traceback": traceback.format_exc()},
                status=500,
            )

    def create_job(self, kind: str, title: str, target) -> None:
        payload = self.read_json()
        job = JOB_STORE.create(kind=kind, title=title, payload=payload)
        start_thread(job.id, target, payload)
        self.write_json({"job": asdict(job)}, status=202)

    def read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("content-length") or "0")
        raw_body = self.rfile.read(length) if length else b"{}"
        if not raw_body:
            return {}
        payload = json.loads(raw_body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise WebApiError("JSON body must be an object")
        return payload

    def serve_static(self, request_path: str) -> None:
        relative = request_path.removeprefix("/static/")
        target = (STATIC_ROOT / relative).resolve()
        if not str(target).startswith(str(STATIC_ROOT.resolve())):
            self.write_json({"error": "Invalid static path"}, status=400)
            return
        if not target.exists() or not target.is_file():
            self.write_json({"error": "Static file not found"}, status=404)
            return
        content_type = {
            ".css": "text/css; charset=utf-8",
            ".js": "application/javascript; charset=utf-8",
            ".html": "text/html; charset=utf-8",
        }.get(target.suffix, "application/octet-stream")
        self.serve_file(target, content_type)

    def serve_file(self, path: Path, content_type: str) -> None:
        body = path.read_bytes()
        self.send_response(200)
        self.send_header("content-type", content_type)
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def write_json(self, payload: dict[str, Any], status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("content-type", "application/json; charset=utf-8")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def load_catalog_backups() -> list[dict[str, Any]]:
    catalog = BackupCatalog(path=str(CATALOG_PATH))
    backups = catalog.list_backups(limit=100)
    return [summarize_backup(item) for item in backups]


def summarize_backup(item: dict[str, Any]) -> dict[str, Any]:
    validation_status = item.get("validation_status") or {}
    restore_validation = item.get("restore_validation") or {}
    return {
        "id": item.get("id") or item.get("backup_id"),
        "database_name": item.get("database_name"),
        "engine": item.get("engine") or item.get("database_type"),
        "backup_type": item.get("backup_type") or item.get("type"),
        "status": item.get("status"),
        "timestamp_start": item.get("timestamp_start"),
        "timestamp_end": item.get("timestamp_end"),
        "duration_seconds": item.get("duration_seconds"),
        "backup_location": item.get("backup_location"),
        "metadata_path": item.get("metadata_path"),
        "validation_path": item.get("validation_path"),
        "validation_state": validation_status.get("state"),
        "recovery_status": validation_status.get("recovery_status"),
        "restore_validation_state": restore_validation.get("state"),
        "backup_size_bytes": item.get("backup_size_bytes")
        or item.get("statistics", {}).get("total_size_bytes"),
    }


def load_backup_document(query: dict[str, list[str]]) -> dict[str, Any]:
    backup_id = (query.get("id") or [""])[0]
    document_type = (query.get("type") or ["validation"])[0]
    if document_type not in {"metadata", "validation"}:
        raise WebApiError("Document type must be metadata or validation")
    if not backup_id:
        raise WebApiError("Backup id is required")

    catalog = BackupCatalog(path=str(CATALOG_PATH))
    backup = catalog.get_backup_by_id(backup_id)
    if not backup:
        raise WebApiError("Backup not found")

    path_value = backup.get(f"{document_type}_path")
    if not path_value and backup.get("backup_location"):
        path_value = str(Path(backup["backup_location"]) / f"{document_type}.json")
    if not path_value:
        raise WebApiError(f"{document_type}.json path is absent")

    path = Path(path_value)
    if not path.exists():
        raise WebApiError(f"Document not found: {path}")
    return {
        "path": str(path),
        "document": json.loads(path.read_text(encoding="utf-8")),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Database Backup Utility Web UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    os.chdir(PROJECT_ROOT)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    server = ThreadingHTTPServer((args.host, args.port), RequestHandler)
    print(f"Database Backup Utility Web UI: http://{args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
