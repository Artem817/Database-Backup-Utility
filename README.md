# Database Backup Utility - Physical Backup with WAL Archiving

## Current Implementation Status

**Working Features:**
- PostgreSQL full backup using `pg_basebackup` with WAL streaming
- PostgreSQL differential backup by collecting archived WAL files from a configured `archive_directory`
- PostgreSQL restore pipeline from full backup + archived WAL replay (`restore_command`, `recovery.signal`, startup, `SELECT 1`)
- PostgreSQL PITR restore with `--target-time`, catalog-based base backup selection, and `recovery_target_time`
- PostgreSQL automated end-to-end restore validation after full backups:
  temporary instance, restore, per-table `COUNT(*)`, checksum comparison, and `validation.json` result
- Basic PostgreSQL WAL validation (sequence gaps, timeline consistency, file sanity checks)
- MySQL full backup using `xtrabackup` (Percona XtraBackup)
- MySQL differential backup command implemented via `xtrabackup --incremental` against the last full backup
- Minimal MySQL restore from an XtraBackup directory (`prepare`, `copy-back`, `mysqld`, `mysqladmin ping`)
- **Encrypted credential profiles (MySQL login-path, PostgreSQL .pgpass)**
- SQLite-backed backup catalog tracking and versioned per-backup metadata/validation artifacts
- Optional single `.tar.zst` archive creation for full backups when `zstd` is available
- Local Web UI for dashboard, backup execution, restore validation, job logs, and metadata/validation viewing

**Not Yet Implemented:**
- Cloud storage integration (CLI placeholders exist, upload implementation does not)
- MySQL restore from compressed `.qp` artifacts or single `.tar.zst` archives
- Production-grade error handling
- Stable user-facing PostgreSQL incremental backup workflow

**Known Issues:**
- Direct filesystem access is required for MySQL physical backups and PostgreSQL archived WAL access
- Limited testing across different PostgreSQL/MySQL versions
- No automated cleanup of old backups
- WAL validation is basic; PostgreSQL full backups now also use restore validation
- Naive PITR target times are interpreted as UTC; timestamps with offsets are normalized to UTC

This utility is a work in progress and **not recommended for production use** at this time.

## Architecture

The utility uses native database backup tools instead of SQL dumps:

**PostgreSQL:**
- Full backup: `pg_basebackup` with tar format and gzip compression
- Differential backup: copies archived WAL files from a configured `archive_directory`
- **WAL chain validation** (sequence gaps, timeline consistency, file integrity — **basic**)
- Requires REPLICATION privilege, `wal_level = replica`, and access to the configured WAL archive directory

**MySQL:**
- Full backup: `xtrabackup` physical backup with compression
- Differential backup command: `xtrabackup --incremental --incremental-basedir=<last full backup>`
- Minimal restore command: `xtrabackup --prepare`, `xtrabackup --copy-back`, start isolated `mysqld`
- Requires Percona XtraBackup installed and access to the MySQL data directory

## Metadata and Catalog

Backups now use a unified metadata contract and a SQL-backed catalog layer.

- `metadata.json` is versioned and stores normalized backup fields such as `engine`, `backup_type`, `parent_backup_id`, `base_backup_id`, artifact paths, LSN/binlog markers, checksums, manifest/checkpoint pointers, and validation state.
- `validation.json` is generated alongside `metadata.json` and records schema validation, required artifact presence, marker summary, and recovery status.
- `backup_catalog.db` is the source of truth for backup history and chain resolution.
- If `backup_catalog.db` is empty and a legacy `backup_catalog.json` exists, catalog records are imported automatically on first use.
- Backup chain lookup is now filtered by `database_name`, so parent/base backup resolution does not mix different databases that share the same catalog.

## Validation Model

Each completed backup is persisted through the same contract layer.

- PostgreSQL metadata includes LSN/WAL markers when available.
- MySQL metadata includes binlog markers when available.
- Artifact checksums are calculated for key backup files.
- Required artifacts are validated per engine and backup type before the backup is marked as completed in the catalog.
- PostgreSQL full backups run an automated test-restore by default. The restored database is compared with the source database using per-table row counts and deterministic row JSON checksums.
- The restore validation report is embedded in `validation.json` under `restore_validation`; the compact status is mirrored in `recovery_status`.
- If backup output directory creation succeeded but later stages fail, the backup can still produce formal `metadata.json` / `validation.json` records with failed validation state.

## Requirements

- Python 3.10 or higher
- PostgreSQL client tools (pg_basebackup)
- MySQL: Percona XtraBackup 8.0
- Optional: `zstd` and `tar` for single-file `.tar.zst` archives
- User permissions:
  - PostgreSQL: REPLICATION privilege
  - PostgreSQL: Access to the configured WAL archive directory
  - MySQL: Standard backup privileges + read access to data directory

## Installation

```bash
git clone https://github.com/<your_user>/<repo_name>.git
cd <repo_name>
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Configuration Methods

The utility supports three configuration methods:

### 1. Encrypted Profile Configuration (Recommended) 🔐

**Most secure option** - Credentials are encrypted and never exposed in environment variables or command line.

#### MySQL Login-Path Setup

MySQL uses `mysql_config_editor` to store encrypted credentials in `~/.mylogin.cnf`:

```bash
# Create a login-path profile
mysql_config_editor set --login-path=xtrabackup \
  --host=localhost \
  --user=backup_user \
  --password
# Enter password when prompted

# Verify the profile (password is obfuscated)
mysql_config_editor print --all

# Test connection
mysql --login-path=xtrabackup -e "SELECT VERSION();"
```

#### PostgreSQL .pgpass Setup

PostgreSQL uses `~/.pgpass` file for password storage:

```bash
# Create .pgpass file in your home directory (~)
echo "localhost:5432:*:backup_user:your_password" >> ~/.pgpass

# Set correct permissions (required)
chmod 0600 ~/.pgpass

# Test connection (no password prompt)
psql -h localhost -U backup_user -d postgres
```

📌 Replace `backup_user` and `your_password` with your actual credentials.

More information: [PostgreSQL .pgpass documentation](https://www.postgresql.org/docs/current/libpq-pgpass.html)

**Usage with profile configuration:**

```bash
# PostgreSQL with .pgpass
python cli/dbtool.py backup --db postgres --database mydb \
  --storage local --config profile

# MySQL with login-path
python cli/dbtool.py backup --db mysql --database mydb \
  --storage local --config profile
```

📌 Use `--config profile` parameter to read encrypted credentials.

### 2. Environment File Configuration (.env)

Traditional method using a local `.env` file. Create it manually in the project root (an `.env.example` template is not currently committed).

Configure `.env` with your database credentials:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=yourpassword
DB_NAME=yourdatabase
```

**Usage:**

```bash
python cli/dbtool.py backup --db postgres --database mydb \
  --storage local --config file
```

⚠️ **Security Warning:** The PostgreSQL documentation (Section 32.15) states that using `PGPASSWORD` environment variable is not recommended because other users can see process environment variables.  
[PostgreSQL 18 / Environment Variables](https://www.postgresql.org/docs/current/libpq-envars.html)

### 3. Manual Command Line Configuration

Pass credentials directly via command line (least secure):

```bash
python cli/dbtool.py backup --db postgres --database mydb \
  --storage local --config manual \
  --host localhost --port 5432 \
  --user backup_user --password secret
```

⚠️ **Security Warning:** Passwords in command line arguments are visible in process lists and shell history. Use profile or file configuration instead.

## PostgreSQL Setup

Grant replication privilege:
```sql
ALTER USER your_user REPLICATION;
```

Ensure `wal_level` is set correctly in `postgresql.conf`:
```
wal_level = replica
```

Configure WAL archiving so PostgreSQL copies segments into an archive directory:
```
archive_mode = on
archive_command = 'cp %p /path/to/archive/%f'
```

Restart PostgreSQL after configuration changes.

On first PostgreSQL run, the utility asks you to confirm or save that `archive_directory` in `~/.backup_utility/config.json`.

## MySQL Setup

Install Percona XtraBackup:

```bash
# Ubuntu/Debian
wget https://repo.percona.com/apt/percona-release_latest.generic_all.deb
sudo dpkg -i percona-release_latest.generic_all.deb
sudo apt-get update
sudo apt-get install percona-xtrabackup-80

# macOS
brew install percona-xtrabackup

# Verify
xtrabackup --version
```

## Usage

⚠️ **Important: Filesystem Access Requirements**

Operations that require direct filesystem access need elevated privileges:

**Operations requiring sudo/elevated permissions:**
- MySQL `xtrabackup` full or differential backup (reads MySQL data files)
- PostgreSQL differential backup when the configured WAL archive directory is protected by OS permissions

**Operations NOT requiring sudo:**
- PostgreSQL `pg_basebackup` full backup (uses replication protocol)

### Running with elevated permissions:

**macOS/Linux:**
```bash
# MySQL full backup (requires sudo)
sudo python cli/dbtool.py backup --db mysql --database mydb --storage local --config profile

# PostgreSQL differential backup (may require sudo or postgres user)
sudo python cli/dbtool.py backup --db postgres --database mydb --storage local --config profile

# PostgreSQL full backup (no sudo needed - uses replication protocol)
python cli/dbtool.py backup --db postgres --database mydb --storage local --config profile
```

**Windows:**
- Run Command Prompt or PowerShell as Administrator
- Or grant read permissions to database data directories for your user account

**Alternative to sudo:**
- Run as database user: `sudo -u postgres python cli/dbtool.py ...` or `sudo -u mysql python cli/dbtool.py ...`
- Grant read permissions to the MySQL data directory / configured PostgreSQL archive directory (one-time setup)

**Security Note:** When using `sudo` with encrypted profile configuration, credentials remain secure and are never exposed in process lists.

---

### Available commands:

```bash
# Full database backup
full database -path /path/to/backups

# PostgreSQL full backup with explicit restore validation flags
full database -path /path/to/backups \
  -validate-restore true \
  -restore-validation-port 0 \
  -restore-validation-checksums true

# Differential backup
# PostgreSQL: archived WAL copy with basic validation
# MySQL: xtrabackup incremental from the last full backup
differential backup

# PostgreSQL restore from interactive console
restore -path /path/to/full_backup_dir -data-dir /path/to/new_restore_data -wal-path /path/to/archive_directory

# PostgreSQL PITR restore from interactive console
restore -target-time "2026-05-04 12:30:00" -data-dir /path/to/new_restore_data -wal-path /path/to/archive_directory

# Execute SQL query
SQL SELECT * FROM users WHERE id < 100

# Export query results to CSV
SQL SELECT * FROM users -extract -path /tmp/exports

# Show help
help

# Exit
exit
```

## Local Web UI

The project also includes a lightweight local control panel built on Python's standard HTTP server. It does not require Flask, FastAPI, or frontend build tooling.

```bash
source .venv/bin/activate
python -m webui.server
```

Open:

```text
http://127.0.0.1:8765
```

The Web UI provides:
- backup catalog dashboard from `backup_catalog.db`
- PostgreSQL/MySQL full backup form
- PostgreSQL test-restore validation form
- background job status and live job logs
- `metadata.json` and `validation.json` viewer

For PostgreSQL backups, pass the WAL archive directory in the form. The web server does not use the CLI prompt flow for WAL archive configuration.

For PostgreSQL restore, `-wal-path` should point to the WAL archive directory used by `restore_command`, not to a single WAL segment file.

## Backup Structure

Backups are created directly under the path passed to `full database -path ...`. Differential backups are created as sibling directories next to the last full backup. Exact contents depend on the database type:

```
/backups/
├── full_mydb_20251105_150000_a1b2/
│   ├── metadata.json
│   ├── validation.json
│   ├── base.tar.gz                  # PostgreSQL full backup
│   ├── pg_wal.tar.gz                # PostgreSQL WAL at backup time
│   ├── backup_manifest              # PostgreSQL 13+, optional
│   ├── xtrabackup_checkpoints       # MySQL full backup metadata
│   └── ...
├── full_mydb_20251105_150000_a1b2.tar.zst  # Optional single archive
└── differential_mydb_20251105_160000_c3d4/
    ├── base_backup_id.txt
    ├── metadata.json
    ├── validation.json
    ├── 0000000100000000000000A1     # PostgreSQL archived WAL files
    ├── xtrabackup_checkpoints       # MySQL differential backup metadata
    └── ...
```

Other on-disk artifacts:
- `backup_catalog.db` stores backup history across runs
- `backup_<database>.log` stores per-database logs

`metadata.json` and `validation.json` are now part of the normal backup contract, not optional diagnostics. Restore and automated test-restore logic consume these files together with `backup_catalog.db`.

## PostgreSQL Automated Restore Validation

PostgreSQL full backups run an end-to-end validation pipeline by default.

What the validation does:
- captures source table signatures for the selected database
- creates a temporary PostgreSQL data directory
- restores the backup into a temporary PostgreSQL instance on a separate port
- runs SQL verification against the restored instance
- compares source and restored table lists, `COUNT(*)`, and table checksums
- stops and removes the temporary restore instance unless `-restore-validation-keep-data true` is passed
- stores the full result in the backup's `validation.json`

Useful flags:

```bash
full database -path /backups/postgres -validate-restore true
full database -path /backups/postgres -restore-validation-port 55440
full database -path /backups/postgres -restore-validation-checksums false
full database -path /backups/postgres -restore-validation-keep-data true
```

If restore validation is enabled and fails, the backup contract is marked as failed even if `pg_basebackup` created files successfully. This is intentional: a backup that cannot be restored is not treated as a valid backup.

For high-write databases, run validation during a quiet/maintenance window or expect legitimate source-vs-restored mismatches from writes committed after the backup recovery point.

## PostgreSQL Restore

Minimal PostgreSQL restore is now available through a dedicated CLI command. It is intentionally scoped to the current PostgreSQL full backup format produced by this utility: `base.tar.gz` + `pg_wal.tar.gz`, plus archived WAL files reachable through `restore_command`.

```bash
python cli/dbtool.py restore --db postgres --database postgres \
  --backup-path /path/to/full_backup_dir \
  --data-dir /path/to/new_restore_data \
  --archive-dir /path/to/archive_directory \
  --restore-port 55432
```

For PITR, pass `--target-time`. Timestamps without timezone are treated as UTC. Timestamps with an offset are normalized to UTC before backup selection and before writing `recovery_target_time`. If `--backup-path` is omitted, the utility selects the newest completed PostgreSQL full backup for the database whose backup completion time is at or before the target time:

```bash
python cli/dbtool.py restore --db postgres --database postgres \
  --target-time "2026-05-04 12:30:00" \
  --data-dir /path/to/new_restore_data \
  --archive-dir /path/to/archive_directory
```

What this command does:
- extracts the full backup into a new PostgreSQL data directory
- replays WAL through `restore_command` from the supplied archive directory
- when `--target-time` is set, writes UTC-normalized `recovery_target_time`, `recovery_target_action = 'promote'`, and `recovery_target_timeline = 'latest'`
- creates `recovery.signal`
- starts PostgreSQL with `pg_ctl`
- verifies the restored instance with `SELECT 1;`

If `--archive-dir` is omitted, the utility tries to use the saved PostgreSQL WAL archive path from `~/.backup_utility/config.json`.

## MySQL Restore

Minimal MySQL restore is available for an already-expanded XtraBackup directory:

```bash
python cli/dbtool.py restore --db mysql \
  --backup-path /path/to/mysql_backup \
  --data-dir /tmp/mysql_restore \
  --port 3308
```

The backup directory must contain `xtrabackup_info`, `xtrabackup_checkpoints`, `ibdata1`, and `mysql/`. If those artifacts are stored as `.zst` files, the restore flow recursively decompresses them with `zstd -d --keep` before validation and prepare. It then runs `xtrabackup --prepare`, `xtrabackup --copy-back`, adjusts ownership when possible, starts an isolated `mysqld` on `127.0.0.1:<port>`, and verifies it with `mysqladmin ping`.

## Limitations

**Current Limitations:**
- No partial/table-level backups (full database only)
- PostgreSQL differential backup depends on a preconfigured WAL archive directory
- No retention policy management
- No background scheduling or orchestration; most operations are still sequential
- Limited error recovery

**PostgreSQL:**
- Differential backup may require running as `postgres` user or equivalent permissions
- Differential backup depends on filesystem access to the configured WAL archive directory
- Restore currently targets the standard full backup layout (`base.tar.gz` + `pg_wal.tar.gz`) and does not support tablespace tar archives
- PITR depends on a complete WAL archive reachable through `restore_command` and currently uses PostgreSQL 12+ `recovery.signal` style recovery

**MySQL:**
- Full backups and differential backups are implemented
- Differential backup is based on `xtrabackup --incremental` against the last full backup
- Minimal restore expects an XtraBackup directory containing `xtrabackup_info`, `xtrabackup_checkpoints`, `ibdata1`, and `mysql/`
- Restore recursively decompresses `.zst` artifacts before `xtrabackup --prepare`
- Restore from compressed `.qp` artifacts or single `.tar.zst` archives is not implemented

## Development Status

This project is currently paused. Current follow-up areas if development resumes:

- Hardening the MySQL differential / incremental-from-full workflow
- Hardening PITR restore validation and WAL coverage diagnostics
- Improving error handling and validation
- Hardening PostgreSQL restore validation for very large/high-write databases
- Testing across more database versions
- Improving documentation
- Deciding whether to expose the experimental PostgreSQL incremental/WAL pipeline code currently living under `services/backup/incremential/` and `services/wal/`

## Troubleshooting

**PostgreSQL differential backup fails with permission denied:**

The differential flow needs read access to the configured PostgreSQL archive directory. Options:
- Run as the postgres user: `sudo -u postgres python cli/dbtool.py ...`
- Run with sudo: `sudo python cli/dbtool.py ...`
- Grant read permissions to the configured archive directory (security consideration required)

**MySQL xtrabackup fails with permission denied:**

```
xtrabackup: Can't change dir to '/usr/local/mysql/data/' (OS errno 13 - Permission denied)
```

xtrabackup needs read access to MySQL data directory. Options:
- Run with sudo: `sudo python cli/dbtool.py ...`
- Run as mysql user: `sudo -u mysql python cli/dbtool.py ...`
- Grant read permissions to data directory (security consideration required)

**pg_basebackup: must be superuser or replication role:**

```sql
ALTER USER your_user REPLICATION;
```

**wal_level error:**

Edit `postgresql.conf`:
```
wal_level = replica
```
Then restart PostgreSQL.

**xtrabackup command not found:**

Install Percona XtraBackup as shown in the MySQL Setup section.

## Contributing

This project is experimental. Contributions are welcome, particularly:

- Testing on different PostgreSQL/MySQL versions
- Hardening MySQL differential / incremental-from-full backups
- Testing PITR restore on different PostgreSQL versions and timezone settings
- Automated restore functionality
- Backup verification tools
- Error handling improvements
- Documentation

Please note that APIs and project structure may change if development resumes.

## License

MIT License - see LICENSE file

## Disclaimer

This software is provided as-is, without warranty. It is not production-ready and should only be used in development/testing environments. Always verify your backups and test restore procedures before relying on this tool.
