from abc import ABC, abstractmethod
from pathlib import Path
import json

from services.backup.metadata import BackupMetadataReader


class IDifferentialBackupStrategy(ABC):
    @abstractmethod
    def perform_differential_backup(
        self, metadata_reader: BackupMetadataReader
    ) -> dict | None:
        """Performs a differential backup based on the provided metadata reader."""
        raise NotImplementedError


class DifferentialBackupStrategyBase(IDifferentialBackupStrategy):
    """
    Reusable helpers shared by concrete differential backup strategies.
    Provides metadata writing and safe finalization helpers.
    """

    def __init__(self, logger, messenger):
        self._logger = logger
        self._messenger = messenger

    def write_metadata_file(self, metadata: dict, output_path: Path) -> bool:
        """Writes backup metadata to a JSON file in the destination directory."""
        try:
            metadata_file = output_path / "metadata.json"
            with open(metadata_file, "w") as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False, default=str)
            self._messenger.info(f"Metadata saved: {metadata_file}")
            return True
        except Exception as e:  # pragma: no cover - logging side effect
            self._messenger.error(f"Failed to write metadata file: {e}")
            self._logger.error(f"Failed to write metadata file: {e}")
            return False

    @staticmethod
    def _calculate_dir_size(path: Path) -> int:
        """Calculate total size (in bytes) of files under a directory."""
        return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())

    def finalize_backup(
        self,
        metadata: dict,
        output_dir: Path,
        success: bool,
        extra_metadata: dict | None = None,
    ) -> bool:
        """
        Persist metadata to disk and close the logger transaction.

        Args:
            metadata: Metadata dictionary to persist.
            output_dir: Destination directory for metadata.json.
            success: Backup result flag.
            extra_metadata: Optional additional fields to merge before persisting.
        """
        if extra_metadata:
            metadata.update(extra_metadata)

        metadata.setdefault("backup_location", str(output_dir))
        metadata.setdefault("backup_size_bytes", self._calculate_dir_size(output_dir))

        self.write_metadata_file(metadata, output_dir)
        self._logger.finish_backup(metadata, success=success)
        return success
