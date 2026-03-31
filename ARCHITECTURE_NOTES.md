# Architecture Notes / Open Concerns

This is a quick brain dump of things that look fragile or would benefit from follow-up before calling the project "done."

## What worries me
- **Naming inconsistencies:** packages like `incremential_*` and `conection_config_mixin.py` are misspelled. Imports work today but hurt discoverability and will confuse new contributors.
- **Coupled responsibilities:** `PostgresClient` / `MySQLClient` mix connection lifecycle, CLI I/O, WAL configuration, and backup orchestration. A thinner client + dedicated “backup runner”/“config” objects would be easier to test and evolve.
- **Subprocess execution safety:** calls to `pg_basebackup`, `xtrabackup`, and shell utilities lack timeouts and structured error handling (stderr is logged but not parsed). Long-running or hung processes could block the CLI indefinitely.
- **Credential exposure:** MySQL incremental backup appends `--password=` on the command line, which is visible in process lists. Prefer environment variables or a my.cnf include file.
- **Catalog/metadata mutation:** metadata is treated as free-form dicts across services. There is no schema validation, so missing/typoed keys will only be noticed at runtime.
- **Artifacts in repo:** `__pycache__` and `.DS_Store` files sit under `services/` and `clients/`; they should be ignored to keep the repo clean.
- **Testing gap:** no automated tests around the WAL pipeline, WAL validation, or differential strategies. That makes refactors risky (especially around WAL chain validation and copy logic).

## Quick wins to consider next
- Add a lightweight dataclass (or pydantic model) for backup metadata to validate required fields before writing `metadata.json`.
- Introduce subprocess wrappers with timeouts and redaction for sensitive arguments.
- Add a small integration test harness that feeds fake WAL files into `WalArchiverPipeline` to validate stage ordering and error paths.
- Normalize package naming (e.g., rename `incremential` -> `incremental`, `conection_config_mixin` -> `connection_config_mixin`) with import shims to preserve compatibility.
