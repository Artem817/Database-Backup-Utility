# services/wal/pipeline/stage_integrity.py
import hashlib
from pathlib import Path
from services.wal.pipeline.context import WalFileContext


class IntegrityStage:
    """
    Checks the readability of WAL in the backup directory + calculates SHA256.

    This is NOT a logical check of the WAL format.
    This is physical integrity (disk/read + checksum).
    """

    def __init__(self, logger, messenger=None, chunk_size: int = 1024 * 1024):
        self._logger = logger
        self._messenger = messenger
        self._chunk_size = chunk_size

    def execute(self, ctx: WalFileContext) -> bool:
        file_path = Path(ctx.current_path)

        if not file_path.exists():
            raise FileNotFoundError(f"IntegrityStage: WAL not found: {file_path}")

        size = file_path.stat().st_size
        if size <= 0:
            raise ValueError(f"IntegrityStage: WAL empty/corrupted: {file_path.name}")

        if size % ctx.segment_size != 0:
            raise ValueError(
                f"IntegrityStage: WAL size invalid: {size} bytes "
                f"(expected multiple of {ctx.segment_size})"
            )

        hasher = hashlib.sha256()
        with file_path.open("rb") as f:
            while True:
                chunk = f.read(self._chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)

        ctx.checksum = hasher.hexdigest()
        ctx.size_bytes = size

        self._logger.info(
            f"IntegrityStage: {file_path.name} OK, SHA256={ctx.checksum[:12]}..."
        )
        if self._messenger:
            self._messenger.info(f"Integrity verified for {file_path.name}")

        return True
