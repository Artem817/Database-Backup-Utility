WAL Pipeline
============

The WAL archiver copies PostgreSQL WAL segments from the archive directory into a differential backup directory and records metadata for each segment.

Pipeline entry point
--------------------
- ``WalArchiverPipeline`` (``services/wal/pipeline/pipeline.py``) accepts a list of WAL filenames plus source/destination directories and processes them through four stages. Statistics (processed/bytes/errors) are returned alongside per-file metadata.

Stages
------
- ``WalFileStabilityValidator`` – waits/retries until the archived WAL exists and reaches the expected segment size (default 16MB) to avoid copying incomplete files.
- ``AtomicWriteStage`` – copies to a temporary file, fsyncs, and atomically renames into the destination, preventing partial writes on crashes.
- ``IntegrityStage`` – reopens the copied WAL, validates size alignment, and calculates a SHA256 checksum.
- ``JournalStage`` – appends a structured record (filename, size, checksum) into the metadata list for later persistence.

Supporting types
----------------
- ``WalFileContext`` – carries paths, segment size, checksum, and the accumulating metadata list through the pipeline stages.
- ``WalChainValidation`` – used by the PostgreSQL differential strategy to ensure the archived WAL sequence is continuous, on the correct timeline, and readable before invoking the pipeline.

Operational notes
-----------------
- The pipeline raises on fatal errors (e.g., integrity failure) and accumulates skipped file counts in ``PipelineStats`` so the caller can react.
- Archiver safety relies on the archive directory already being configured in PostgreSQL (``archive_mode=on`` and ``archive_command`` pointing to the same path used here).
