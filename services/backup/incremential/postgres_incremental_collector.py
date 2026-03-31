from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple


@dataclass
class IncrementalBackupContext:
    start_lsn: str
    end_lsn: str

    previous_wal_file: str
    current_wal_file: str

    archive_dir: Path
    backup_dir: Path

    parent_metadata: Dict[str, Any]

    wal_files: Optional[List[str]] = None
    wal_metadata_items: Optional[List[Dict[str, Any]]] = None


class PostgresIncrementalCollector:
    """
    Strategy -> Resolver -> ChainValidation -> Pipeline -> MetadataWriter
    """

    def __init__(
        self,
        connection_provider,
        logger,
        messenger,
        resolver,
        chain_validator_cls,
        pipeline,
        metadata_writer,
    ):
        self._cp = connection_provider
        self._logger = logger
        self._messenger = messenger

        self._resolver = resolver
        self._chain_validator_cls = chain_validator_cls
        self._pipeline = pipeline
        self._metadata_writer = metadata_writer

    def _switch_and_get_bounds(self) -> Tuple[str, str]:
        """
        Returns (end_lsn, current_wal_file).
        """
        with self._cp.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_switch_wal();")
                # return value is LSN, ignore

                cur.execute("SELECT pg_current_wal_lsn();")
                end_lsn = cur.fetchone()[0]

                cur.execute("SELECT pg_walfile_name(%s::pg_lsn);", (end_lsn,))
                current_wal_file = cur.fetchone()[0]

        return end_lsn, current_wal_file

    def run(self, metadata_reader, base_outpath: str | Path) -> bool:
        base_outpath = Path(base_outpath)

        parent = metadata_reader.get_successful_backup()
        if not parent:
            self._messenger.error("No successful parent backup found.")
            return False

        start_lsn = parent.get("current_lsn")
        previous_wal_file = parent.get("current_wal_file")

        if not start_lsn or not previous_wal_file:
            self._messenger.error("Parent metadata missing current_lsn/current_wal_file.")
            return False

        archive_dir = Path(self._cp.archive_path)
        if not archive_dir.exists() or not archive_dir.is_dir():
            self._messenger.error(f"Invalid WAL archive dir: {archive_dir}")
            return False

        end_lsn, current_wal_file = self._switch_and_get_bounds()

        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        database_name = parent.get("database_name") or parent.get("database") or "database"
        backup_id = f"incremental_{database_name}_{ts}"
        backup_dir = base_outpath / backup_id
        backup_dir.mkdir(parents=True, exist_ok=True)

        ctx = IncrementalBackupContext(
            start_lsn=start_lsn,
            end_lsn=end_lsn,
            previous_wal_file=previous_wal_file,
            current_wal_file=current_wal_file,
            archive_dir=archive_dir,
            backup_dir=backup_dir,
            parent_metadata=parent,
        )

        wal_files = self._resolver.resolve(
            start_lsn=start_lsn,
            end_lsn=end_lsn,
            validate_sequence=False,
        )
        ctx.wal_files = wal_files

        if not wal_files:
            ctx.wal_metadata_items = []
            self._metadata_writer.execute(ctx)
            return True

        validator = self._chain_validator_cls(
            archived_wal_files=wal_files,
            last_full_backup_wal_file=previous_wal_file,
            current_wal_file=current_wal_file,
            wal_archive_directory=archive_dir,
            logger=self._logger,
            messenger=self._messenger,
        )

        if not validator.timeline_consistency_check():
            return False
        if not validator.validate_sequence_gaps():
            return False
        if not validator.basic_wal_file_sanity_check():
            return False

        metadata_items, stats = self._pipeline.process_wal_files(
            wal_files=wal_files,
            archive_dir=archive_dir,
            backup_dir=backup_dir,
        )
        ctx.wal_metadata_items = metadata_items

        self._metadata_writer.execute(ctx)
        return True
