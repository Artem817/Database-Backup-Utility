from __future__ import annotations

import getpass
import os
import shutil
import subprocess
import time
from pathlib import Path

try:
    import grp
    import pwd
except ImportError:  # pragma: no cover - non-POSIX platform fallback
    grp = None
    pwd = None


class MysqlRestoreService:
    REQUIRED_FILES = ("xtrabackup_info", "xtrabackup_checkpoints", "ibdata1")
    REQUIRED_DIRECTORIES = ("mysql",)

    def __init__(
        self,
        backup_path: str,
        data_dir: str,
        restore_port: int = 3308,
        socket_path: str = "/tmp/mysql_restore.sock",
        mysql_bin_dir: str | None = None,
        owner_user: str | None = None,
        owner_group: str | None = None,
        verify_timeout_seconds: int = 30,
        logger=None,
        messenger=None,
    ) -> None:
        self._backup_path = Path(backup_path).expanduser().resolve()
        self._data_dir = Path(data_dir).expanduser().resolve()
        self._restore_port = int(restore_port)
        self._socket_path = Path(socket_path).expanduser().resolve()
        self._mysql_bin_dir = (
            Path(mysql_bin_dir).expanduser().resolve() if mysql_bin_dir else None
        )
        self._owner_user = owner_user
        self._owner_group = owner_group
        self._verify_timeout_seconds = int(verify_timeout_seconds)
        self._logger = logger
        self._messenger = messenger
        self._process: subprocess.Popen | None = None
        self._mysqld_user: str | None = None

    def restore(self) -> bool:
        started = False
        try:
            self._validate_inputs()
            self._decompress_zstd_artifacts()
            self._validate_backup_directory(self._backup_path)
            self._prepare_backup()
            self._prepare_data_directory()
            self._copy_back()
            self._set_data_directory_owner()
            self._start_mysql()
            started = True
            self._verify_mysql()

            self._message_success("MySQL restore pipeline completed successfully")
            self._message_info(f"Restored data directory: {self._data_dir}")
            self._message_info(f"Restore port: {self._restore_port}")
            self._message_info(f"Restore socket: {self._socket_path}")
            self._message_info(
                "Stop command: "
                f"{self._binary_path('mysqladmin')} --protocol=tcp "
                f"-h 127.0.0.1 -P {self._restore_port} shutdown"
            )
            return True
        except Exception as error:
            self._message_error(f"MySQL restore failed: {error}")
            if started:
                self.stop()
            return False

    def stop(self) -> bool:
        if self._process is None or self._process.poll() is not None:
            return True

        self._message_info("Stopping restored MySQL instance")
        self._process.terminate()
        try:
            self._process.wait(timeout=15)
            self._message_success("Restored MySQL instance stopped")
            return True
        except subprocess.TimeoutExpired:
            self._message_warning("mysqld did not stop after terminate; killing process")
            self._process.kill()
            self._process.wait(timeout=15)
            return False

    def _validate_inputs(self) -> None:
        if not self._backup_path.exists():
            raise FileNotFoundError(f"Backup path not found: {self._backup_path}")

        if not self._backup_path.is_dir():
            raise ValueError(
                "MySQL restore currently expects an XtraBackup directory, not an archive"
            )

        if self._data_dir.exists() and not self._data_dir.is_dir():
            raise ValueError(f"Restore data path is not a directory: {self._data_dir}")

        if self._data_dir.exists() and any(self._data_dir.iterdir()):
            raise ValueError(f"Restore data directory must be empty: {self._data_dir}")

        if self._socket_path.exists():
            raise ValueError(f"MySQL restore socket path already exists: {self._socket_path}")

        self._require_binary("xtrabackup")
        self._require_binary("mysqld")
        self._require_binary("mysqladmin")

    def _decompress_zstd_artifacts(self) -> None:
        zstd_files = sorted(self._backup_path.rglob("*.zst"))
        if not zstd_files:
            return

        zstd_binary = self._zstd_binary_path()
        self._message_info(
            f"Decompressing {len(zstd_files)} MySQL .zst backup artifact(s)"
        )

        for zstd_file in zstd_files:
            output_path = zstd_file.with_suffix("")
            if output_path.exists():
                self._message_info(
                    f"Skipping {zstd_file}; decompressed file already exists"
                )
                continue

            command = [
                zstd_binary,
                "-d",
                "--keep",
                str(zstd_file),
                "-o",
                str(output_path),
            ]
            self._message_info(f"Decompressing {zstd_file} -> {output_path}")
            self._run_checked(command, f"zstd decompression failed for {zstd_file}")

    def _validate_backup_directory(self, backup_dir: Path) -> None:
        missing: list[str] = []

        for filename in self.REQUIRED_FILES:
            artifact = backup_dir / filename
            if not artifact.is_file():
                missing.append(filename)

        for directory_name in self.REQUIRED_DIRECTORIES:
            artifact = backup_dir / directory_name
            if not artifact.is_dir():
                missing.append(f"{directory_name}/")

        if missing:
            compressed_hint = ""
            if any(backup_dir.glob("*.qp")) or any(backup_dir.rglob("*.qp")):
                compressed_hint = (
                    " The backup appears to contain compressed .qp files; decompress it "
                    "before using this minimal restore pipeline."
                )
            raise FileNotFoundError(
                "MySQL backup directory is missing required XtraBackup artifacts: "
                f"{', '.join(missing)}.{compressed_hint}"
            )

    def _prepare_backup(self) -> None:
        command = [
            self._binary_path("xtrabackup"),
            "--prepare",
            f"--target-dir={self._backup_path}",
        ]
        self._message_info("Preparing MySQL backup with xtrabackup")
        self._run_checked(command, "xtrabackup prepare failed")

    def _prepare_data_directory(self) -> None:
        self._data_dir.mkdir(parents=True, exist_ok=True)

    def _copy_back(self) -> None:
        command = [
            self._binary_path("xtrabackup"),
            "--copy-back",
            f"--target-dir={self._backup_path}",
            f"--datadir={self._data_dir}",
        ]
        self._message_info("Copying prepared backup into restore data directory")
        self._run_checked(command, "xtrabackup copy-back failed")

    def _set_data_directory_owner(self) -> None:
        owner = self._detect_owner()
        if owner is None:
            self._message_warning(
                "Could not determine MySQL data directory owner; leaving ownership unchanged"
            )
            return

        owner_user, owner_group = owner
        self._mysqld_user = owner_user

        if not self._running_as_root():
            self._message_info(
                f"Not running as root; using current user ownership for {self._data_dir}"
            )
            return

        owner_spec = f"{owner_user}:{owner_group}" if owner_group else owner_user
        command = ["chown", "-R", owner_spec, str(self._data_dir)]
        self._message_info(f"Setting MySQL restore data directory owner to {owner_spec}")
        self._run_checked(command, "chown restore data directory failed")

    def _detect_owner(self) -> tuple[str, str | None] | None:
        if os.name != "posix":
            return None

        if self._owner_user:
            return (
                self._owner_user,
                self._owner_group or self._default_group_for_user(self._owner_user),
            )

        if self._running_as_root():
            for candidate in ("mysql", "_mysql"):
                if self._user_exists(candidate):
                    return candidate, self._default_group_for_user(candidate)

        current_user = getpass.getuser()
        return current_user, self._default_group_for_user(current_user)

    @staticmethod
    def _running_as_root() -> bool:
        return hasattr(os, "geteuid") and os.geteuid() == 0

    @staticmethod
    def _user_exists(username: str) -> bool:
        if pwd is None:
            return False
        try:
            pwd.getpwnam(username)
            return True
        except KeyError:
            return False

    @staticmethod
    def _default_group_for_user(username: str) -> str | None:
        if pwd is None or grp is None:
            return None
        try:
            user = pwd.getpwnam(username)
        except KeyError:
            return None

        try:
            return grp.getgrgid(user.pw_gid).gr_name
        except KeyError:
            return None

    def _start_mysql(self) -> None:
        self._socket_path.parent.mkdir(parents=True, exist_ok=True)
        log_path = self._data_dir / "restore_mysqld.log"
        command = [
            self._binary_path("mysqld"),
            f"--datadir={self._data_dir}",
            f"--port={self._restore_port}",
            f"--socket={self._socket_path}",
            "--bind-address=127.0.0.1",
            "--skip-networking=0",
        ]
        if self._running_as_root() and self._mysqld_user:
            command.append(f"--user={self._mysqld_user}")

        self._message_info("Starting restored MySQL instance")
        with log_path.open("ab") as log_handle:
            self._process = subprocess.Popen(
                command,
                stdout=log_handle,
                stderr=subprocess.STDOUT,
            )

        time.sleep(1)
        if self._process.poll() is not None:
            raise RuntimeError(
                "mysqld start failed.\n"
                f"log tail:\n{self._read_log_tail(log_path)}"
            )

    def _verify_mysql(self) -> None:
        command = [
            self._binary_path("mysqladmin"),
            "--no-defaults",
            "--protocol=tcp",
            "-h",
            "127.0.0.1",
            "-P",
            str(self._restore_port),
            "ping",
        ]
        deadline = time.monotonic() + self._verify_timeout_seconds
        last_stdout = ""
        last_stderr = ""

        self._message_info("Verifying restored MySQL instance with mysqladmin ping")
        while time.monotonic() < deadline:
            if self._process is not None and self._process.poll() is not None:
                raise RuntimeError(
                    "mysqld exited before verification completed.\n"
                    f"log tail:\n{self._read_log_tail(self._data_dir / 'restore_mysqld.log')}"
                )

            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
            )
            last_stdout = result.stdout.strip()
            last_stderr = result.stderr.strip()
            if result.returncode == 0:
                output = last_stdout or last_stderr or "mysqld is alive"
                self._message_success(f"MySQL verification succeeded: {output}")
                return
            time.sleep(1)

        raise RuntimeError(
            "mysqladmin ping failed.\n"
            f"stdout: {last_stdout}\n"
            f"stderr: {last_stderr}"
        )

    def _run_checked(self, command: list[str], failure_message: str) -> subprocess.CompletedProcess:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"{failure_message}.\n"
                f"stdout: {result.stdout.strip()}\n"
                f"stderr: {result.stderr.strip()}"
            )
        return result

    def _require_binary(self, binary_name: str) -> None:
        binary_path = self._binary_path(binary_name)
        if shutil.which(binary_path) is None:
            raise FileNotFoundError(f"Required binary not found: {binary_path}")

    def _zstd_binary_path(self) -> str:
        path_binary = shutil.which("zstd")
        if path_binary is not None:
            return "zstd"

        if self._mysql_bin_dir:
            candidate = str(self._mysql_bin_dir / "zstd")
            if shutil.which(candidate) is not None:
                return candidate

        raise FileNotFoundError("Required binary not found: zstd")

    def _binary_path(self, binary_name: str) -> str:
        if self._mysql_bin_dir:
            return str(self._mysql_bin_dir / binary_name)
        return binary_name

    def _read_log_tail(self, log_path: Path, line_limit: int = 40) -> str:
        if not log_path.exists():
            return "(restore log not created)"

        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
        if not lines:
            return "(restore log is empty)"
        return "\n".join(lines[-line_limit:])

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
