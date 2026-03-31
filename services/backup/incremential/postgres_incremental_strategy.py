
from services.backup.incremential.postgres_incremental_collector import PostgresIncrementalCollector
from services.wal.pipeline.metadata_writer import IncrementalMetadataWriter
from services.wal.pipeline.pipeline import WalArchiverPipeline
from services.wal.resolver.wal_range_resolver import WalRangeResolver
from services.walvalidation.wal_check import WalChainValidation


class PostgresIncrementalBackupStrategy:
    def __init__(self, connection_provider, logger, messenger):
        self._cp = connection_provider
        self._logger = logger
        self._messenger = messenger

    def perform_incremental_backup(
        self, metadata_reader, outpath: str, storage: str = "local"
    ) -> bool:
        resolver = WalRangeResolver(self._cp, self._messenger, self._logger)
        pipeline = WalArchiverPipeline(self._logger, self._messenger)
        metadata_writer = IncrementalMetadataWriter(self._logger, self._messenger)

        collector = PostgresIncrementalCollector(
            connection_provider=self._cp,
            logger=self._logger,
            messenger=self._messenger,
            resolver=resolver,
            chain_validator_cls=WalChainValidation,
            pipeline=pipeline,
            metadata_writer=metadata_writer,
        )

        return collector.run(metadata_reader=metadata_reader, base_outpath=outpath)
