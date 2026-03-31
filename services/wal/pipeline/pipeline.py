from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict, Any, Tuple

from services.wal.pipeline.context import WalFileContext
from services.wal.pipeline.stage_validate import WalFileStabilityValidator
from services.wal.pipeline.stage_atomic_write import AtomicWriteStage
from services.wal.pipeline.stage_integrity import IntegrityStage
from services.wal.pipeline.stage_journal import JournalStage


@dataclass
class PipelineStats:
    """
    Tracks statistics for the WAL archiving pipeline execution.

    Attributes:
        total_files (int): Total number of files submitted for processing.
        processed_files (int): Number of files successfully processed.
        skipped_files (int): Number of files skipped due to errors.
        total_bytes (int): Total size of processed data in bytes.
        errors (List[str]): List of error messages encountered during processing.
    """
    total_files: int = 0
    processed_files: int = 0
    skipped_files: int = 0
    total_bytes: int = 0
    errors: List[str] = field(default_factory=list)


class WalArchiverPipeline:
    """
    Orchestrates the Write-Ahead Log (WAL) archiving pipeline.

    This pipeline processes a list of WAL files through a sequence of stages:
    1.  **Validation**: Verifies the source WAL file stability and size.
    2.  **Atomic Write**: Safely copies the WAL file to the backup destination.
    3.  **Integrity**: Verifies the integrity of the copied file.
    4.  **Journaling**: collect metadata and statistics.

    Attributes:
        _logger: Logger instance for recording events.
        _messenger: Notification service for alerting on issues.
        _wal_segment_size (int): Expected size of a WAL segment in bytes.
    """

    def __init__(self, logger, messenger=None, wal_segment_size: int = 16 * 1024 * 1024):
        """
        Initialize the pipeline.

        Args:
            logger: Logger object.
            messenger: Optional messenger for notifications.
            wal_segment_size (int): Expected WAL file size. Defaults to 16MB.
        """
        self._logger = logger
        self._messenger = messenger
        self._wal_segment_size = wal_segment_size

        self._validate_stage = WalFileStabilityValidator(
            expected_size=wal_segment_size,
            logger=logger,
            messenger=messenger,
        )
        self._atomic_write_stage = AtomicWriteStage(logger=logger, messenger=messenger)
        self._integrity_stage = IntegrityStage(logger=logger, messenger=messenger)
        self._journal_stage = JournalStage(logger=logger, messenger=messenger)

        self._stats = PipelineStats()
        self._metadata_items: List[Dict[str, Any]] = []

    def process_wal_files(
        self,
        wal_files: List[str],
        archive_dir: Path | str,
        backup_dir: Path | str,
    ) -> Tuple[List[Dict[str, Any]], PipelineStats]:
        """
        Process a batch of WAL files through the archiving pipeline.

        Args:
            wal_files (List[str]): List of WAL filenames to process.
            archive_dir (Path | str): Source directory containing the WAL files.
            backup_dir (Path | str): Destination directory for the archived WALs.

        Returns:
            Tuple[List[Dict[str, Any]], PipelineStats]: A tuple containing:
                - A list of metadata dictionaries for successfully processed files.
                - A PipelineStats object with processing statistics.

        Raises:
            FileNotFoundError: If the archive directory does not exist.
        """
        archive_dir = Path(archive_dir)
        backup_dir = Path(backup_dir)

        if not wal_files:
            self._logger.info("WalArchiverPipeline: no WAL files to process")
            return [], self._stats

        if not archive_dir.exists() or not archive_dir.is_dir():
            raise FileNotFoundError(f"Archive directory invalid: {archive_dir}")

        backup_dir.mkdir(parents=True, exist_ok=True)

        self._stats = PipelineStats(total_files=len(wal_files))
        self._metadata_items = []

        for wal_name in wal_files:
            try:
                self._process_one(wal_name, archive_dir, backup_dir)
                self._stats.processed_files += 1
            except Exception as e:
                self._stats.skipped_files += 1
                msg = f"Pipeline fatal error for {wal_name}: {e}"
                self._stats.errors.append(msg)
                self._logger.error(msg, exc_info=True)
                raise

        return self._metadata_items, self._stats

    def _process_one(self, wal_name: str, archive_dir: Path, backup_dir: Path) -> None:
        """
        Process a single WAL file through all pipeline stages.

        Args:
            wal_name (str): Name of the WAL file.
            archive_dir (Path): Source directory path.
            backup_dir (Path): Destination directory path.

        Raises:
            RuntimeError: If the atomic write stage fails.
            Exception: If validation or other stages fail.
        """
        src_path = archive_dir / wal_name

        # stage 1 validate in archive
        self._validate_stage.validate(src_path)

        ctx = WalFileContext(
            current_path=src_path,
            dest_dir=backup_dir,
            wal_name=wal_name,
            segment_size=self._wal_segment_size,
            metadata_items=self._metadata_items,
        )

        # stage 2 atomic write to backup dir
        ok = self._atomic_write_stage.execute(ctx)
        if not ok:
            raise RuntimeError("AtomicWriteStage returned False")

        # stage 3 integrity
        self._integrity_stage.execute(ctx)

        # stage 4 journal
        record = self._journal_stage.execute(ctx)
        self._stats.total_bytes += record["size_bytes"]
