"""
WAL range resolver for PostgreSQL incremental backups.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Tuple
from dataclasses import dataclass


@dataclass(frozen=True)
class WALSegmentInfo:
    """Parsed representation of a single WAL segment filename."""

    filename: str
    timeline: int
    log_id: int
    segment: int

    @classmethod
    def parse(cls, filename: str) -> Optional["WALSegmentInfo"]:
        """
        Parse a WAL filename of the form:
            TTTTTTTTLLLLLLLLSSSSSSSS  (hex, 24 chars)

        Returns:
            WALSegmentInfo if the filename is valid, otherwise None.
        """
        if len(filename) != 24:
            return None

        try:
            timeline = int(filename[0:8], 16)
            log_id = int(filename[8:16], 16)
            segment = int(filename[16:24], 16)
        except ValueError:
            return None

        return cls(filename=filename, timeline=timeline, log_id=log_id, segment=segment)

    def next_segment(self) -> "WALSegmentInfo":
        """
        Compute the next WAL segment on the same timeline.

        Notes:
            PostgreSQL uses 0x100 (256) segments per log.
        """
        SEGMENTS_PER_LOG = 0x100

        next_segment = self.segment + 1
        next_log = self.log_id

        if next_segment >= SEGMENTS_PER_LOG:
            next_segment = 0
            next_log += 1

        next_filename = (
            f"{self.timeline:08X}{next_log:08X}{next_segment:08X}"
        )
        return WALSegmentInfo(
            filename=next_filename,
            timeline=self.timeline,
            log_id=next_log,
            segment=next_segment,
        )

    def is_next_segment(self, other: "WALSegmentInfo") -> bool:
        """
        Return True if `other` is the immediate next segment after `self`
        on the same timeline.
        """
        if self.timeline != other.timeline:
            return False

        expected = self.next_segment()
        return (
            expected.timeline == other.timeline
            and expected.log_id == other.log_id
            and expected.segment == other.segment
        )


class WALSequenceValidator:
    """
    Basic validator for WAL filename sequences.

    For physical incremental / differential backups we require:
        - a single timeline (no timeline switches),
        - no gaps in the sequence of WAL segment filenames.
    """

    @staticmethod
    def validate(wal_files: List[str]) -> Tuple[bool, Optional[str]]:
        """
        Validate that a list of WAL filenames is contiguous and on a single timeline.

        Args:
            wal_files: list of filenames (must be sorted in ascending order).

        Returns:
            (is_valid, error_message)
                is_valid = True  and error_message is None  → OK
                is_valid = False and error_message is not None → validation failed
        """
        if not wal_files:
            return False, "Empty WAL file list"

        if len(wal_files) == 1:
            info = WALSegmentInfo.parse(wal_files[0])
            if not info:
                return False, f"Invalid WAL filename: {wal_files[0]}"
            return True, None

        parsed: List[WALSegmentInfo] = []
        for filename in wal_files:
            info = WALSegmentInfo.parse(filename)
            if not info:
                return False, f"Invalid WAL filename: {filename}"
            parsed.append(info)

        for i in range(len(parsed) - 1):
            current = parsed[i]
            nxt = parsed[i + 1]

            # For physical incremental backups, a timeline change means the chain
            # is not usable off the same base backup.
            if current.timeline != nxt.timeline:
                return False, (
                    f"Timeline switch detected between {current.filename} and "
                    f"{nxt.filename}. For physical incremental backup this "
                    f"is considered invalid. Take a new FULL backup after "
                    f"timeline promotion."
                )

            if not current.is_next_segment(nxt):
                expected = current.next_segment()
                return False, (
                    "Gap detected in WAL sequence: after "
                    f"{current.filename} expected {expected.filename}, "
                    f"but found {nxt.filename}"
                )

        return True, None


class WalRangeResolver:
    """
    Resolves the list of WAL files that cover the LSN range [start_lsn, end_lsn].

    Responsibility:
        - translate LSN boundaries to WAL filenames via PostgreSQL,
        - list candidate WAL files from the archive directory,
        - filter files that fall strictly between the two filenames
          by lexical order on the same timeline.

    It does NOT perform deep WAL integrity checks (size, checksum, etc.).
    Those should be done by a dedicated validation component.
    """

    WAL_FILENAME_PATTERN = re.compile(r"^[0-9A-F]{24}$")

    def __init__(self, connection_provider, messenger, logger):
        self._connection_provider = connection_provider
        self._messenger = messenger
        self._logger = logger
        self._sequence_validator = WALSequenceValidator()

    def _get_wal_filename_from_lsn(self, cursor, lsn: str) -> str:
        """
        Get WAL filename for a given LSN via pg_walfile_name(lsn).
        """
        cursor.execute("SELECT pg_walfile_name(%s::pg_lsn);", (lsn,))
        result = cursor.fetchone()
        if not result or not result[0]:
            raise ValueError(f"Could not resolve WAL filename for LSN: {lsn}")
        return result[0]

    def _ensure_lsn_order(self, cursor, start_lsn: str, end_lsn: str) -> None:
        """
        Ensure start_lsn <= end_lsn at the database level.

        Raises:
            ValueError if start_lsn > end_lsn.
        """
        cursor.execute(
            "SELECT %s::pg_lsn <= %s::pg_lsn;",
            (start_lsn, end_lsn),
        )
        ok = cursor.fetchone()[0]
        if not ok:
            raise ValueError(
                f"Invalid LSN order: start_lsn ({start_lsn}) must not be greater "
                f"than end_lsn ({end_lsn})"
            )

    def _get_archive_files(self, archive_dir: Path) -> List[str]:
        """
        List candidate WAL filenames from archive_dir.

        Rules:
            - must be a regular file,
            - filename length == 24,
            - filename is [0-9A-F]{24} (pure hex, no suffix),
            - we do NOT check file size or contents here.
        """
        if not archive_dir.exists():
            raise FileNotFoundError(f"Archive directory does not exist: {archive_dir}")
        if not archive_dir.is_dir():
            raise NotADirectoryError(f"Archive path is not a directory: {archive_dir}")

        wal_files: List[str] = []

        try:
            for item in archive_dir.iterdir():
                if not item.is_file():
                    continue

                filename = item.name

                if len(filename) != 24:
                    continue

                if not self.WAL_FILENAME_PATTERN.match(filename):
                    continue

                wal_files.append(filename)

        except PermissionError as e:
            raise PermissionError(
                f"Permission denied when reading archive directory {archive_dir}: {e}"
            )
        except OSError as e:
            raise OSError(
                f"OS error while reading archive directory {archive_dir}: {e}"
            )

        return wal_files

    # Public API

    def resolve(
        self,
        start_lsn: str,
        end_lsn: str,
        validate_sequence: bool = True,
    ) -> List[str]:
        """
        Resolve all WAL files in the archive that cover changes between
        start_lsn and end_lsn.

        Semantics:
            - LSN order is validated at the DB level (start_lsn <= end_lsn).
            - start_wal is treated as the WAL file that was already covered
              by the parent backup; we only collect WAL files STRICTLY after it.
            - end_wal is the upper bound; WAL files lexically <= end_wal
              are included.
            - On an archive where filenames are sorted lexicographically,
              this corresponds to the filename range:
                  (start_wal, end_wal]

        Args:
            start_lsn: LSN of the last successful backup (parent).
            end_lsn:   current LSN at backup time.
            validate_sequence:
                If True, perform basic filename sequence validation
                (single timeline, no gaps). Deep integrity checks should
                still be done by a dedicated component.

        Returns:
            Sorted list of WAL filenames in the resolved range.
            Returns an empty list if there are no WAL files between
            the two LSNs (e.g. same WAL segment and no archived rotation).

        Raises:
            ValueError:       invalid LSN order or invalid WAL sequence
            FileNotFoundError / NotADirectoryError / PermissionError: archive issues
            OSError:          filesystem problems
        """
        try:
            with self._connection_provider.get_connection() as conn:
                with conn.cursor() as cursor:
                    self._ensure_lsn_order(cursor, start_lsn, end_lsn)

                    start_wal = self._get_wal_filename_from_lsn(cursor, start_lsn)
                    end_wal = self._get_wal_filename_from_lsn(cursor, end_lsn)

            self._logger.info(
                f"Resolving WAL files between {start_wal} and {end_wal} "
                f"(LSN: {start_lsn} -> {end_lsn})"
            )

            archive_dir = Path(self._connection_provider.archive_path)

            all_files = self._get_archive_files(archive_dir)
            if not all_files:
                self._logger.warning(
                    f"No valid WAL segments found in archive directory {archive_dir}"
                )
                return []

            all_files.sort()

            wal_files: List[str] = []
            for wal in all_files:
                if start_wal < wal <= end_wal:
                    wal_files.append(wal)

            self._logger.info(
                f"Resolved {len(wal_files)} WAL files in requested range."
            )

            if validate_sequence and wal_files:
                is_valid, error_msg = self._sequence_validator.validate(wal_files)
                if not is_valid:
                    self._messenger.error(
                        f"WAL sequence validation failed: {error_msg}"
                    )
                    self._logger.error(
                        f"WAL sequence validation failed: {error_msg}"
                    )
                    raise ValueError(error_msg)

            if wal_files:
                if wal_files[0] != min(wal_files[0], end_wal) and wal_files[0] != start_wal:
                    self._logger.debug(
                        f"First WAL in range is {wal_files[0]}, "
                        f"which may be later than start_wal={start_wal}"
                    )

                if wal_files[-1] != end_wal:
                    self._logger.debug(
                        f"Last WAL in range is {wal_files[-1]}, "
                        f"but end_wal was {end_wal}. This usually means the "
                        f"final segment has not yet been archived."
                    )

            return wal_files

        except (FileNotFoundError, NotADirectoryError, PermissionError, OSError):
            self._messenger.error("Failed to access WAL archive directory.")
            self._logger.exception("Filesystem error while resolving WAL range.")
            raise

        except ValueError:
            self._messenger.error("WAL range resolution failed due to validation error.")
            self._logger.exception("Validation error while resolving WAL range.")
            raise

        except Exception:
            self._messenger.error("Unexpected error while resolving WAL range.")
            self._logger.exception("Unexpected error while resolving WAL range.")
            raise

    def get_missing_wal_files(
        self,
        wal_files: List[str],
    ) -> List[str]:
        """
        Given a sorted list of WAL filenames on a single timeline,
        compute which segment filenames are missing in the sequence.

        This is intended for diagnostics / reporting, not for core backup logic.
        """
        if not wal_files:
            return []

        parsed = [WALSegmentInfo.parse(name) for name in wal_files]
        parsed = [p for p in parsed if p is not None]

        if len(parsed) < 2:
            return []

        missing: List[str] = []

        current = parsed[0]
        last = parsed[-1]

        while current.filename < last.filename:
            nxt = current.next_segment()
            if nxt.filename not in wal_files:
                missing.append(nxt.filename)
            current = nxt

        if missing:
            self._logger.warning(
                "Detected %d missing WAL segments in sequence. "
                "First few: %s%s",
                len(missing),
                missing[:5],
                "..." if len(missing) > 5 else "",
            )

        return missing
