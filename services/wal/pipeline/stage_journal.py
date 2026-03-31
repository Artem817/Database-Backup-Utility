# services/wal/pipeline/stage_journal.py
from typing import Any, Dict
from services.wal.pipeline.context import WalFileContext


class JournalStage:
    """
    Records that WAL has passed all stages.
    Returns dict for metadata.json.
    """

    def __init__(self, logger, messenger=None):
        self._logger = logger
        self._messenger = messenger

    def execute(self, ctx: WalFileContext) -> Dict[str, Any]:
        if not ctx.checksum or ctx.size_bytes is None:
            raise ValueError(
                "JournalStage: checksum/size missing. "
                "IntegrityStage must run before JournalStage."
            )

        record = {
            "filename": ctx.wal_name,
            "backup_path": str(ctx.current_path),
            "size_bytes": ctx.size_bytes,
            "checksum_sha256": ctx.checksum,
            "status": "archived",
        }

        if ctx.metadata_items is not None:
            ctx.metadata_items.append(record)

        self._logger.info(f"JournalStage: WAL archived: {ctx.wal_name}")
        if self._messenger:
            self._messenger.success(f"WAL archived: {ctx.wal_name}")

        return record
