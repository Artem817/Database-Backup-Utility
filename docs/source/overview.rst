System Overview
===============

The project provides a modular backup utility for PostgreSQL and MySQL with a focus on WAL-based differential backups and external archiving.

Database clients
----------------
- ``PostgresClient`` (``clients/postgres_client.py``) – manages connections, configures WAL archiving, runs ``pg_basebackup`` for full backups, and delegates differential backups via the mixins.
- ``MySQLClient`` (``clients/mysql_client.py``) – connects to MySQL and orchestrates full or incremental backups (via Percona XtraBackup) using the same mixin-driven workflow.
- Shared mixins:
  - ``ConnectionConfigMixin`` – parses CLI/config input.
  - ``BackupCatalogMixin`` – records backups in the catalog.
  - ``DifferentialBackupMixin`` – selects the correct differential strategy based on port/db type.

Backup coordination
-------------------
- ``DifferentialBackupService`` (``services/backup/core.py``) – strategy coordinator that is closed for modification but open for extension through concrete strategies.
- ``DifferentialBackupStrategyBase`` (``services/backup/differential/strategy_base.py``) – shared helpers for writing metadata and finalizing logger transactions.
- ``PostgresDifferentialBackupStrategy`` (``services/backup/differential/strategy/postgres_strategy.py``) – collects WAL files from the archive directory, validates chain continuity, and writes a differential backup directory with metadata.
- ``MySQLDifferentialBackupStrategy`` (``services/backup/differential/strategy/mysql_strategy.py``) – runs ``xtrabackup --incremental`` against the previous full backup and persists metadata.
- ``BackupMetadataReader`` (``services/backup/metadata.py``) – finds the latest full backup, exposes manifest paths, and prints catalog history for the CLI.

WAL processing pipeline
-----------------------
- ``WalArchiverPipeline`` (``services/wal/pipeline/pipeline.py``) – orchestrates staged processing for WAL segments: validation → atomic write → integrity → journaling.
- Stages: ``WalFileStabilityValidator``, ``AtomicWriteStage``, ``IntegrityStage``, ``JournalStage`` (details in ``wal_pipeline.rst``).

Execution helpers
-----------------
- ``QueryExecutor`` (``services/execution/executor.py``) – runs SQL with logging and error propagation.
- ``WalChainValidation`` (``services/walvalidation/wal_check.py``) – verifies WAL sequence, timeline consistency, and basic size/readability before copying.
