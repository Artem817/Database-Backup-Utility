
from dataclasses import dataclass
from pathlib import Path

SEGMENTS_PER_LOG = 0x100          
LOGS_PER_TIMELINE = 0x100000000  

def get_next_wal_segment(current_wal: str) -> str:
    """Calculate the next WAL segment name given the current one."""
    timeline_hex = current_wal[0:8]
    log_hex = current_wal[8:16]
    seg_hex = current_wal[16:24]

    timeline = int(timeline_hex, 16)
    log = int(log_hex, 16)
    seg = int(seg_hex, 16)

    seg += 1
    if seg >= SEGMENTS_PER_LOG:
        seg = 0
        log += 1
        if log >= LOGS_PER_TIMELINE:
            log = 0
            timeline += 1

    return f"{timeline:08X}{log:08X}{seg:08X}"

class WalChainValidation:
    def __init__(
        self,
        archived_wal_files: list[str],
        last_full_backup_wal_file: str,
        current_wal_file: str,
        wal_archive_directory: str | Path,
        logger,
        messenger,
    ):
        self.archived_wal_files: list[str] = sorted(archived_wal_files)

        self.last_full_backup_wal_file: str = last_full_backup_wal_file
        self.current_wal_file: str = current_wal_file
        self.wal_archive_directory: Path = Path(wal_archive_directory)

        self._logger = logger
        self._messenger = messenger

    def _iter_relevant_wal_files(self) -> list[str]:
        """
        Returns only those WALs that lie within the range:
        (last_full_backup_wal_file, current_wal_file]
            
        """
        return [
            wal
            for wal in self.archived_wal_files
            if self.last_full_backup_wal_file < wal <= self.current_wal_file
        ]

    def validate_sequence_gaps(self) -> bool:
        """
        Validate that there are no gaps in the WAL segment sequence
        between the last full backup WAL file and the current WAL file.

        IMPORTANT:
        - works correctly only if archived_wal_files are sorted
        - considers only the range (last_full_backup_wal_file, current_wal_file]
        """

        relevant_wal_files = self._iter_relevant_wal_files()

        if not relevant_wal_files:
            self._logger.info(
                "No WAL files found between last full backup and current WAL; "
                "nothing to validate."
            )
            return True

        expected_wal = get_next_wal_segment(self.last_full_backup_wal_file)

        for wal in relevant_wal_files:
            wal_path = self.wal_archive_directory / wal

            if expected_wal < wal:
                self._logger.error(
                    f"Detected gap in WAL chain. First missing segment: {expected_wal}"
                )
                self._messenger.error(
                    f"Missing WAL segment in archive (first missing: {expected_wal}). "
                    "Differential backup cannot be trusted. Please take a new FULL backup."
                )
                return False

            if expected_wal == wal:
                if not wal_path.exists():
                    self._logger.error(f"WAL listed but does not exist on disk: {wal}")
                    self._messenger.error(f"WAL file missing on disk: {wal}")
                    return False

                expected_wal = get_next_wal_segment(expected_wal)
                continue

            if wal < expected_wal:
                continue

        return True

    def timeline_consistency_check(self) -> bool:
        """
        Перевіряє, що всі WAL в діапазоні знаходяться на тій самій timeline,
        що й last_full_backup_wal_file та current_wal_file.
        """
        expected_timeline = self.last_full_backup_wal_file[0:8]

        current_timeline = self.current_wal_file[0:8]
        if current_timeline != expected_timeline:
            self._logger.error(
                f"Timeline conflict: current WAL {self.current_wal_file} has "
                f"timeline {current_timeline}, expected {expected_timeline}"
            )
            self._messenger.error(
                "Timeline conflict detected between full backup and current WAL. "
                "Please create a NEW full backup after timeline switch."
            )
            return False

        relevant_wal_files = self._iter_relevant_wal_files()

        for wal in relevant_wal_files:
            timeline = wal[0:8]
            if timeline != expected_timeline:
                self._logger.error(
                    f"Fatal timeline conflict detected: {wal} "
                    f"(expected timeline = {expected_timeline}, got {timeline})"
                )
                self._messenger.error(
                    f"Fatal timeline conflict detected for WAL: {wal}. "
                    "This typically means failover/promote happened. "
                    "Take a fresh FULL backup."
                )
                return False

        return True

    def basic_wal_file_sanity_check(self, wal_segment_size: int = 16 * 1024 * 1024) -> bool:
        """
        Basic WAL file sanity check (без розбору формату WAL).

        Checks for all WALs in the relevant range:
        - the file exists;
        - size > 0;
        - size = N * wal_segment_size (typically 16MB);
        - the file can be read without error (like a normal file).
        """
        relevant_wal_files = self._iter_relevant_wal_files()

        for wal in relevant_wal_files:
            wal_path = self.wal_archive_directory / wal

            # File exists
            if not wal_path.exists():
                self._logger.error(f"WAL file does not exist: {wal}")
                self._messenger.error(f"WAL file missing: {wal}")
                return False

            # Size > 0
            try:
                size = wal_path.stat().st_size
            except OSError as e:
                self._logger.error(f"Failed to stat WAL file {wal}: {e}")
                self._messenger.error(f"Cannot access WAL file: {wal}")
                return False

            if size <= 0:
                self._logger.error(f"WAL file is corrupted (non-positive size): {wal}")
                self._messenger.error(f"WAL file appears to be corrupted (size <= 0): {wal}")
                return False

            # Size = N * wal_segment_size
            if size % wal_segment_size != 0:
                self._logger.error(
                    f"WAL file has unexpected size: {wal} "
                    f"(size={size}, segment_size={wal_segment_size})"
                )
                self._messenger.error(
                    f"WAL file has invalid size (not multiple of {wal_segment_size}): {wal}"
                )
                return False

            # Size can be read without errors 
            try:
                with wal_path.open("rb") as f:
                    for _ in iter(lambda: f.read(1024 * 1024), b""):
                        pass
            except OSError as e:
                self._logger.error(f"Failed to read WAL file {wal}: {e}")
                self._messenger.error(f"Cannot read WAL file: {wal}")
                return False

        return True
