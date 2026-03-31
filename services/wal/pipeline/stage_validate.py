# services/wal/pipeline/stage_validate.py
from pathlib import Path
from utility.resilience import TransientError, retry_with_backoff


class WalFileStabilityValidator:
    """
    Checks that the WAL in the archive:
        - exists
        - is not zero
        - has the expected size
        - (for v1.0.0) we assume that size == segment_size

        TransientError -> retry
        ValueError     -> fatal problem
    """

    def __init__(self, expected_size=16 * 1024 * 1024, logger=None, messenger=None):
        self._expected_size = expected_size
        self._logger = logger
        self._messenger = messenger

    @property
    def expected_size(self) -> int:
        return self._expected_size

    @retry_with_backoff(max_retries=5, initial_delay=0.5, backoff_factor=2.0)
    def validate(self, file_path: Path) -> bool:
        if not file_path.exists():
            raise TransientError(f"WAL not found yet: {file_path}")

        size = file_path.stat().st_size

        if size == 0:
            raise TransientError(f"WAL is empty (archiving in progress?): {file_path}")

        if size < self.expected_size:
            raise TransientError(
                f"WAL incomplete (size {size} < {self.expected_size}): {file_path}"
            )

        # For v1.0.0, we strictly require exactly expected_size.
        if size != self.expected_size:
            raise ValueError(
                f"Invalid WAL size {size} bytes, expected {self.expected_size}: {file_path}"
            )

        return True
