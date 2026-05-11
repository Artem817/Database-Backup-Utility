from __future__ import annotations

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from services.restore.mysql_restore import MysqlRestoreService


class DummyLogger:
    def info(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str) -> None:
        pass


class DummyMessenger:
    def info(self, message: str) -> None:
        pass

    def success(self, message: str) -> None:
        pass

    def warning(self, message: str) -> None:
        pass

    def error(self, message: str) -> None:
        pass


class MysqlRestoreServiceTests(unittest.TestCase):
    def _build_backup_dir(self, root: Path) -> Path:
        backup_dir = root / "mysql_backup"
        backup_dir.mkdir()
        (backup_dir / "xtrabackup_info").write_text("uuid = test\n", encoding="utf-8")
        (backup_dir / "xtrabackup_checkpoints").write_text(
            "backup_type = full-backuped\n",
            encoding="utf-8",
        )
        (backup_dir / "ibdata1").write_bytes(b"innodb")
        (backup_dir / "mysql").mkdir()
        return backup_dir

    def _build_zstd_backup_dir(self, root: Path) -> Path:
        backup_dir = root / "mysql_backup_zstd"
        backup_dir.mkdir()
        (backup_dir / "xtrabackup_info.zst").write_bytes(b"compressed info")
        (backup_dir / "xtrabackup_checkpoints").write_text(
            "backup_type = full-backuped\n",
            encoding="utf-8",
        )
        (backup_dir / "ibdata1.zst").write_bytes(b"compressed innodb")
        mysql_dir = backup_dir / "mysql"
        mysql_dir.mkdir()
        (mysql_dir / "user.ibd.zst").write_bytes(b"compressed table")
        return backup_dir

    def _build_service(
        self,
        backup_dir: Path,
        data_dir: Path,
        socket_path: Path,
    ) -> MysqlRestoreService:
        return MysqlRestoreService(
            backup_path=str(backup_dir),
            data_dir=str(data_dir),
            restore_port=3308,
            socket_path=str(socket_path),
            owner_user="mysql",
            owner_group="mysql",
            logger=DummyLogger(),
            messenger=DummyMessenger(),
        )

    def test_validate_backup_directory_requires_core_xtrabackup_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            backup_dir = self._build_backup_dir(root)
            service = self._build_service(
                backup_dir,
                root / "data",
                root / "mysql_restore.sock",
            )

            service._validate_backup_directory(backup_dir)

            (backup_dir / "ibdata1").unlink()
            with self.assertRaisesRegex(FileNotFoundError, "ibdata1"):
                service._validate_backup_directory(backup_dir)

    @patch("services.restore.mysql_restore.time.sleep", return_value=None)
    @patch("services.restore.mysql_restore.os.geteuid", return_value=0)
    @patch("services.restore.mysql_restore.shutil.which", side_effect=lambda binary: binary)
    @patch("services.restore.mysql_restore.subprocess.Popen")
    @patch("services.restore.mysql_restore.subprocess.run")
    def test_restore_runs_prepare_copy_back_chown_start_and_ping(
        self,
        run_mock,
        popen_mock,
        which_mock,
        geteuid_mock,
        sleep_mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            backup_dir = self._build_backup_dir(root)
            data_dir = root / "restore_data"
            socket_path = root / "mysql_restore.sock"
            service = self._build_service(backup_dir, data_dir, socket_path)

            run_mock.return_value = subprocess.CompletedProcess(
                args=[],
                returncode=0,
                stdout="mysqld is alive",
                stderr="",
            )
            process = MagicMock()
            process.poll.return_value = None
            popen_mock.return_value = process

            self.assertTrue(service.restore())

            run_commands = [call.args[0] for call in run_mock.call_args_list]
            self.assertIn(
                ["xtrabackup", "--prepare", f"--target-dir={backup_dir}"],
                run_commands,
            )
            self.assertIn(
                [
                    "xtrabackup",
                    "--copy-back",
                    f"--target-dir={backup_dir}",
                    f"--datadir={data_dir}",
                ],
                run_commands,
            )
            self.assertIn(
                ["chown", "-R", "mysql:mysql", str(data_dir)],
                run_commands,
            )
            self.assertIn(
                [
                    "mysqladmin",
                    "--no-defaults",
                    "--protocol=tcp",
                    "-h",
                    "127.0.0.1",
                    "-P",
                    "3308",
                    "ping",
                ],
                run_commands,
            )

            popen_command = popen_mock.call_args.args[0]
            self.assertEqual(
                popen_command,
                [
                    "mysqld",
                    f"--datadir={data_dir}",
                    "--port=3308",
                    f"--socket={socket_path}",
                    "--bind-address=127.0.0.1",
                    "--skip-networking=0",
                    "--user=mysql",
                ],
            )

    @patch("services.restore.mysql_restore.time.sleep", return_value=None)
    @patch("services.restore.mysql_restore.os.geteuid", return_value=0)
    @patch("services.restore.mysql_restore.shutil.which", side_effect=lambda binary: binary)
    @patch("services.restore.mysql_restore.subprocess.Popen")
    @patch("services.restore.mysql_restore.subprocess.run")
    def test_restore_decompresses_zstd_artifacts_before_prepare(
        self,
        run_mock,
        popen_mock,
        which_mock,
        geteuid_mock,
        sleep_mock,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            backup_dir = self._build_zstd_backup_dir(root)
            data_dir = root / "restore_data"
            socket_path = root / "mysql_restore.sock"
            service = self._build_service(backup_dir, data_dir, socket_path)

            def run_side_effect(command, *args, **kwargs):
                if command[0] == "zstd":
                    output_path = Path(command[command.index("-o") + 1])
                    output_path.write_bytes(b"decompressed")
                return subprocess.CompletedProcess(
                    args=command,
                    returncode=0,
                    stdout="mysqld is alive",
                    stderr="",
                )

            run_mock.side_effect = run_side_effect
            process = MagicMock()
            process.poll.return_value = None
            popen_mock.return_value = process

            self.assertTrue(service.restore())

            self.assertTrue((backup_dir / "xtrabackup_info").exists())
            self.assertTrue((backup_dir / "ibdata1").exists())
            self.assertTrue((backup_dir / "mysql" / "user.ibd").exists())

            run_commands = [call.args[0] for call in run_mock.call_args_list]
            zstd_commands = [
                (index, command)
                for index, command in enumerate(run_commands)
                if command[0] == "zstd"
            ]
            prepare_index = next(
                index
                for index, command in enumerate(run_commands)
                if command[0] == "xtrabackup" and "--prepare" in command
            )

            self.assertEqual(len(zstd_commands), 3)
            self.assertTrue(all(index < prepare_index for index, _ in zstd_commands))
            self.assertIn(
                [
                    "zstd",
                    "-d",
                    "--keep",
                    str(backup_dir / "xtrabackup_info.zst"),
                    "-o",
                    str(backup_dir / "xtrabackup_info"),
                ],
                run_commands,
            )
            self.assertIn(
                [
                    "zstd",
                    "-d",
                    "--keep",
                    str(backup_dir / "ibdata1.zst"),
                    "-o",
                    str(backup_dir / "ibdata1"),
                ],
                run_commands,
            )
            self.assertIn(
                [
                    "zstd",
                    "-d",
                    "--keep",
                    str(backup_dir / "mysql" / "user.ibd.zst"),
                    "-o",
                    str(backup_dir / "mysql" / "user.ibd"),
                ],
                run_commands,
            )


if __name__ == "__main__":
    unittest.main()
