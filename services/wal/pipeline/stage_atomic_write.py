import os
import uuid
import shutil
from pathlib import Path
from services.wal.pipeline.context import WalFileContext


class AtomicWriteStage:
    """
    Ensures that backup_dir will not contain partially written WALs.

    Algorithm:
    1) Copy the file to tmp
    2) fsync tmp
    3) Atomic rename tmp -> dest
    4) (Best effort) fsync directory
    """

    def __init__(self, logger, messenger=None, chunk_size: int = 1024 * 1024):
        self._logger = logger
        self._messenger = messenger
        self._chunk_size = chunk_size

    def execute(self, ctx: WalFileContext) -> bool:
        src: Path = ctx.current_path
        dest_dir: Path = ctx.dest_dir
        tmp_dir: Path = dest_dir / "_tmp"

        dest_dir.mkdir(parents=True, exist_ok=True)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        dest = dest_dir / src.name
        tmp = tmp_dir / f"{src.name}.{uuid.uuid4().hex}.tmp"

        try:
            with src.open("rb") as f_in, tmp.open("xb") as f_out:
                shutil.copyfileobj(f_in, f_out, length=self._chunk_size)
                f_out.flush()
                os.fsync(f_out.fileno())

            tmp.replace(dest)

            # best-effort fsync dir (may not work on Windows)
            try:
                dir_fd = os.open(dest_dir, os.O_DIRECTORY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except Exception:
                pass

            ctx.final_path = dest
            ctx.current_path = dest

            self._logger.info(f"AtomicWriteStage: WAL written atomically: {dest.name}")
            return True

        except FileExistsError:
            self._logger.error(f"AtomicWriteStage: WAL already exists: {dest}")
            if self._messenger:
                self._messenger.error(f"WAL already exists in backup: {dest.name}")
            if tmp.exists():
                tmp.unlink(missing_ok=True)
            return False

        except Exception as e:
            self._logger.error(f"AtomicWriteStage failed for {src.name}: {e}", exc_info=True)
            if self._messenger:
                self._messenger.error(f"Atomic write failed for WAL: {src.name}")

            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass

            return False