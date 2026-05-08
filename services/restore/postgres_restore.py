from __future__ import annotations

import json
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

from services.backup.archive_utils import extract_archive
from services.restore.pitr import format_recovery_target_time, parse_recovery_target_time


class PostgresRestoreService:
    CONFIG_BLOCK_START = "# DBTOOL_POSTGRES_RESTORE_BEGIN"
    CONFIG_BLOCK_END = "# DBTOOL_POSTGRES_RESTORE_END"
    HBA_BLOCK_START = "# DBTOOL_POSTGRES_RESTORE_HBA_BEGIN"
    HBA_BLOCK_END = "# DBTOOL_POSTGRES_RESTORE_HBA_END"

    def __init__(
        self,
        backup_path: str,
        data_dir: str,
        archive_dir: str,
        restore_port: int,
        verify_database: str,
        verify_user: str = "postgres",
        verify_query: str = "SELECT 1;",
        pg_bin_dir: str | None = None,
        target_time: str | None = None,
        logger=None,
        messenger=None,
    ) -> None:
        self._backup_path = Path(backup_path).expanduser().resolve()
        self._data_dir = Path(data_dir).expanduser().resolve()
        self._archive_dir = Path(archive_dir).expanduser().resolve()
        self._restore_port = int(restore_port)
        self._verify_database = verify_database
        self._verify_user = verify_user
        self._verify_query = verify_query
        self._pg_bin_dir = Path(pg_bin_dir).expanduser().resolve() if pg_bin_dir else None
        self._target_time = (
            format_recovery_target_time(parse_recovery_target_time(target_time))
            if target_time
            else None
        )
        self._logger = logger
        self._messenger = messenger
        self._extract_root: Path | None = None

    def restore(self) -> bool:
        try:
            self._validate_inputs()

            backup_dir = self._resolve_backup_directory()
            self._validate_backup_directory(backup_dir)
            self._prepare_data_directory()

            self._extract_tar_archive(backup_dir / "base.tar.gz", self._data_dir)
            self._extract_tar_archive(backup_dir / "pg_wal.tar.gz", self._data_dir)

            self._data_dir.chmod(0o700)
            self._write_restore_configuration()
            self._prepend_local_hba_rules()
            self._create_recovery_signal()

            self._start_postgresql()
            self._verify_select()

            self._message_success("PostgreSQL restore pipeline completed successfully")
            self._message_info(f"Restored data directory: {self._data_dir}")
            self._message_info(f"Restore port: {self._restore_port}")
            if self._target_time:
                self._message_info(f"PITR recovery target time: {self._target_time}")
            self._message_info(
                f"Stop command: {self._binary_path('pg_ctl')} -D {self._data_dir} stop"
            )
            return True
        except Exception as error:
            self._message_error(f"Restore failed: {error}")
            return False
        finally:
            self._cleanup_extract_root()

    def stop(self) -> bool:
        try:
            if not self._data_dir.exists():
                return True

            postmaster_pid = self._data_dir / "postmaster.pid"
            if not postmaster_pid.exists():
                self._message_info("Restored PostgreSQL instance is not running")
                return True

            command = [
                self._binary_path("pg_ctl"),
                "-D",
                str(self._data_dir),
                "-w",
                "stop",
                "-m",
                "fast",
            ]

            self._message_info("Stopping restored PostgreSQL instance")
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                self._message_warning(
                    "pg_ctl stop failed. "
                    f"stdout: {result.stdout.strip()} stderr: {result.stderr.strip()}"
                )
                return False

            self._message_success("Restored PostgreSQL instance stopped")
            return True
        except Exception as error:
            self._message_warning(f"Failed to stop restored PostgreSQL instance: {error}")
            return False

    def _validate_inputs(self) -> None:
        if not self._backup_path.exists():
            raise FileNotFoundError(f"Backup path not found: {self._backup_path}")

        if not self._archive_dir.exists():
            raise FileNotFoundError(f"Archive directory not found: {self._archive_dir}")

        if not self._archive_dir.is_dir():
            raise ValueError(f"Archive path is not a directory: {self._archive_dir}")

        if self._data_dir.exists() and any(self._data_dir.iterdir()):
            raise ValueError(
                f"Restore data directory must be empty: {self._data_dir}"
            )

        self._require_binary("pg_ctl")
        self._require_binary("psql")

    def _resolve_backup_directory(self) -> Path:
        if self._backup_path.is_dir():
            return self._backup_path

        if self._backup_path.name.endswith(".tar.zst"):
            extract_root = Path(tempfile.mkdtemp(prefix="dbtool_pg_restore_"))
            self._extract_root = extract_root

            self._message_info(f"Extracting single archive backup: {self._backup_path}")
            extracted = extract_archive(
                archive_path=self._backup_path,
                output_dir=extract_root,
                logger=self._logger,
                messenger=self._messenger,
            )
            if not extracted:
                raise RuntimeError(f"Failed to extract archive: {self._backup_path}")

            return self._locate_extracted_backup_dir(extract_root)

        raise ValueError(
            "Backup path must point to a PostgreSQL full backup directory or a .tar.zst archive"
        )

    def _locate_extracted_backup_dir(self, extract_root: Path) -> Path:
        if (extract_root / "base.tar.gz").exists():
            return extract_root

        child_dirs = [item for item in extract_root.iterdir() if item.is_dir()]
        for child_dir in child_dirs:
            if (child_dir / "base.tar.gz").exists():
                return child_dir

        raise FileNotFoundError(
            f"No extracted PostgreSQL backup directory found under {extract_root}"
        )

    def _validate_backup_directory(self, backup_dir: Path) -> None:
        base_tar = backup_dir / "base.tar.gz"
        wal_tar = backup_dir / "pg_wal.tar.gz"

        if not base_tar.exists():
            raise FileNotFoundError(f"Full backup is missing base.tar.gz: {base_tar}")

        if not wal_tar.exists():
            raise FileNotFoundError(f"Full backup is missing pg_wal.tar.gz: {wal_tar}")

        metadata_path = backup_dir / "metadata.json"
        if metadata_path.exists():
            with metadata_path.open("r", encoding="utf-8") as handle:
                metadata = json.load(handle)

            engine = metadata.get("engine") or metadata.get("database_type")
            backup_type = metadata.get("backup_type") or metadata.get("type")

            if engine and engine != "postgresql":
                raise ValueError(
                    f"Restore only supports PostgreSQL backups, found engine={engine}"
                )
            if backup_type and backup_type != "full":
                raise ValueError(
                    f"Restore only supports full backups, found backup_type={backup_type}"
                )

        unsupported_tablespaces = [
            item.name
            for item in backup_dir.glob("*.tar.gz")
            if item.name not in {"base.tar.gz", "pg_wal.tar.gz"}
        ]
        if unsupported_tablespaces:
            raise ValueError(
                "Restore for pg_basebackup tablespace tar archives is not implemented. "
                f"Unsupported artifacts: {', '.join(sorted(unsupported_tablespaces))}"
            )

    def _prepare_data_directory(self) -> None:
        self._data_dir.mkdir(parents=True, exist_ok=True)

    def _extract_tar_archive(self, archive_path: Path, target_dir: Path) -> None:
        self._message_info(f"Extracting {archive_path.name} -> {target_dir}")

        with tarfile.open(archive_path, "r:gz") as archive:
            members = archive.getmembers()
            self._assert_members_within_target(target_dir, members)
            archive.extractall(path=target_dir)

    def _assert_members_within_target(self, target_dir: Path, members: list[tarfile.TarInfo]) -> None:
        target_root = target_dir.resolve()

        for member in members:
            candidate = (target_dir / member.name).resolve()
            try:
                candidate.relative_to(target_root)
            except ValueError as error:
                raise ValueError(
                    f"Refusing to extract archive member outside target directory: {member.name}"
                ) from error

    def _write_restore_configuration(self) -> None:
        auto_conf = self._data_dir / "postgresql.auto.conf"
        existing = auto_conf.read_text(encoding="utf-8") if auto_conf.exists() else ""
        existing = self._strip_managed_block(
            existing,
            self.CONFIG_BLOCK_START,
            self.CONFIG_BLOCK_END,
        ).rstrip()

        restore_command = self._build_restore_command()
        config_lines = [
            self.CONFIG_BLOCK_START,
            f"restore_command = '{restore_command}'",
            "archive_mode = off",
        ]
        if self._target_time:
            target_time = self._escape_pg_conf_value(self._target_time)
            config_lines.extend(
                [
                    f"recovery_target_time = '{target_time}'",
                    "recovery_target_action = 'promote'",
                    "recovery_target_timeline = 'latest'",
                ]
            )
        config_lines.extend(
            [
                f"port = {self._restore_port}",
                "listen_addresses = '127.0.0.1'",
                self.CONFIG_BLOCK_END,
            ]
        )
        block = "\n".join(config_lines)

        new_content = block if not existing else f"{existing}\n\n{block}"
        auto_conf.write_text(f"{new_content}\n", encoding="utf-8")
        self._message_info(f"Updated restore settings in {auto_conf}")

    def _build_restore_command(self) -> str:
        archive_file = self._archive_dir / "%f"
        command = f'cp "{archive_file}" "%p"'
        return self._escape_pg_conf_value(command)

    @staticmethod
    def _escape_pg_conf_value(value: str) -> str:
        return value.replace("'", "''")

    def _prepend_local_hba_rules(self) -> None:
        pg_hba_path = self._data_dir / "pg_hba.conf"
        if not pg_hba_path.exists():
            raise FileNotFoundError(f"pg_hba.conf not found in restored data dir: {pg_hba_path}")

        existing = pg_hba_path.read_text(encoding="utf-8")
        existing = self._strip_managed_block(
            existing,
            self.HBA_BLOCK_START,
            self.HBA_BLOCK_END,
        ).lstrip()

        block = "\n".join(
            [
                self.HBA_BLOCK_START,
                "local   all             all                                     trust",
                "host    all             all             127.0.0.1/32            trust",
                "host    all             all             ::1/128                 trust",
                self.HBA_BLOCK_END,
                "",
            ]
        )

        pg_hba_path.write_text(f"{block}{existing}", encoding="utf-8")
        self._message_info(f"Prepended local restore access rules to {pg_hba_path}")

    def _create_recovery_signal(self) -> None:
        recovery_signal = self._data_dir / "recovery.signal"
        recovery_signal.write_text("", encoding="utf-8")
        self._message_info(f"Created {recovery_signal}")

    def _start_postgresql(self) -> None:
        log_path = self._data_dir / "restore_server.log"
        command = [
            self._binary_path("pg_ctl"),
            "-D",
            str(self._data_dir),
            "-l",
            str(log_path),
            "-w",
            "start",
        ]

        self._message_info("Starting restored PostgreSQL instance")
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            log_tail = self._read_log_tail(log_path)
            raise RuntimeError(
                "pg_ctl start failed.\n"
                f"stdout: {result.stdout.strip()}\n"
                f"stderr: {result.stderr.strip()}\n"
                f"log tail:\n{log_tail}"
            )

    def _verify_select(self) -> bool:
        command = [
            self._binary_path("psql"),
            "-h",
            "127.0.0.1",
            "-p",
            str(self._restore_port),
            "-U",
            self._verify_user,
            "-d",
            self._verify_database,
            "-w",
            "-tA",
            "-c",
            self._verify_query,
        ]

        self._message_info(
            f"Running verification query on restored instance: {self._verify_query}"
        )
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            stdout = result.stdout.strip()
            stderr = result.stderr.strip()
            self._message_warning(
                "Verification SELECT failed.\n"
                f"stdout: {stdout}\n"
                f"stderr: {stderr}\n"
                "Warning: this typically happens when the verification user does not exist "
                "in the restored cluster (for example, the backup was taken with a different "
                "login role than the default 'postgres'). The restore can still be successful "
                "even if this verification query fails."
            )
            return False

        output = result.stdout.strip() or "(no output)"
        self._message_success(f"Verification SELECT succeeded: {output}")
        return True

    def _require_binary(self, binary_name: str) -> None:
        binary_path = self._binary_path(binary_name)
        if shutil.which(binary_path) is None:
            raise FileNotFoundError(f"Required binary not found: {binary_path}")

    def _binary_path(self, binary_name: str) -> str:
        if self._pg_bin_dir:
            return str(self._pg_bin_dir / binary_name)
        return binary_name

    def _read_log_tail(self, log_path: Path, line_limit: int = 40) -> str:
        if not log_path.exists():
            return "(restore log not created)"

        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        if not lines:
            return "(restore log is empty)"
        return "\n".join(lines[-line_limit:])

    def _strip_managed_block(self, content: str, start_marker: str, end_marker: str) -> str:
        start_index = content.find(start_marker)
        end_index = content.find(end_marker)
        if start_index == -1 or end_index == -1:
            return content

        end_index += len(end_marker)
        stripped = f"{content[:start_index]}{content[end_index:]}"
        return stripped.strip("\n")

    def _cleanup_extract_root(self) -> None:
        if self._extract_root and self._extract_root.exists():
            shutil.rmtree(self._extract_root, ignore_errors=True)

    def _message_info(self, message: str) -> None:
        if self._messenger is not None:
            self._messenger.info(message)
        if self._logger is not None:
            self._logger.info(message)

    def _message_success(self, message: str) -> None:
        if self._messenger is not None:
            self._messenger.success(message)
        if self._logger is not None:
            self._logger.info(message)

    def _message_warning(self, message: str) -> None:
        if self._messenger is not None:
            self._messenger.warning(message)
        if self._logger is not None:
            self._logger.warning(message)

    def _message_error(self, message: str) -> None:
        if self._messenger is not None:
            self._messenger.error(message)
        if self._logger is not None:
            self._logger.error(message)