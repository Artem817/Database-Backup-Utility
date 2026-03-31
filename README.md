# Database Backup Utility - Physical Backup with WAL Archiving

## Current Implementation Status

**Working Features:**
- PostgreSQL full backup using `pg_basebackup` with WAL streaming
- PostgreSQL differential backup by collecting archived WAL files from a configured `archive_directory`
- Basic PostgreSQL WAL validation (sequence gaps, timeline consistency, file sanity checks)
- MySQL full backup using `xtrabackup` (Percona XtraBackup)
- MySQL differential backup command implemented via `xtrabackup --incremental` against the last full backup
- **Encrypted credential profiles (MySQL login-path, PostgreSQL .pgpass)**
- Backup catalog tracking and per-backup metadata
- Optional single `.tar.zst` archive creation for full backups when `zstd` is available

**Not Yet Implemented:**
- Automated restore functionality
- Point-in-Time Recovery (PITR) restore interface
- Cloud storage integration (CLI placeholders exist, upload implementation does not)
- End-to-end backup verification / automated test-restore workflow
- Production-grade error handling
- Stable user-facing PostgreSQL incremental backup workflow

**Known Issues:**
- Direct filesystem access is required for MySQL physical backups and PostgreSQL archived WAL access
- Limited testing across different PostgreSQL/MySQL versions
- No automated cleanup of old backups
- WAL validation is basic only; there is no full restore verification
- Timezone handling inconsistencies

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
- Requires Percona XtraBackup installed and access to the MySQL data directory

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

# Differential backup
# PostgreSQL: archived WAL copy with basic validation
# MySQL: xtrabackup incremental from the last full backup
differential backup

# Execute SQL query
SQL SELECT * FROM users WHERE id < 100

# Export query results to CSV
SQL SELECT * FROM users -extract -path /tmp/exports

# Show help
help

# Exit
exit
```

## Backup Structure

Backups are created directly under the path passed to `full database -path ...`. Differential backups are created as sibling directories next to the last full backup. Exact contents depend on the database type:

```
/backups/
├── full_mydb_20251105_150000_a1b2/
│   ├── metadata.json
│   ├── base.tar.gz                  # PostgreSQL full backup
│   ├── pg_wal.tar.gz                # PostgreSQL WAL at backup time
│   ├── backup_manifest              # PostgreSQL 13+, optional
│   ├── xtrabackup_checkpoints       # MySQL full backup metadata
│   └── ...
├── full_mydb_20251105_150000_a1b2.tar.zst  # Optional single archive
└── differential_mydb_20251105_160000_c3d4/
    ├── base_backup_id.txt
    ├── metadata.json
    ├── 0000000100000000000000A1     # PostgreSQL archived WAL files
    ├── xtrabackup_checkpoints       # MySQL differential backup metadata
    └── ...
```

Other on-disk artifacts:
- `backup_catalog.json` stores backup history across runs
- `backup_<database>.log` stores per-database logs

## Limitations

**Current Limitations:**
- No partial/table-level backups (full database only)
- PostgreSQL differential backup depends on a preconfigured WAL archive directory
- No automated restore scripts
- No end-to-end restore verification; only basic WAL validation for the PostgreSQL differential flow
- No retention policy management
- No background scheduling or orchestration; most operations are still sequential
- Limited error recovery

**PostgreSQL:**
- Differential backup may require running as `postgres` user or equivalent permissions
- Differential backup depends on filesystem access to the configured WAL archive directory
- PITR restore interface not yet implemented

**MySQL:**
- Full backups and differential backups are implemented
- Differential backup is based on `xtrabackup --incremental` against the last full backup
- Requires xtrabackup prepare step before restore (manual)

## Development Status

This project is currently paused. Current follow-up areas if development resumes:

- Hardening the MySQL differential / incremental-from-full workflow
- Adding PITR restore interface for PostgreSQL
- Improving error handling and validation
- Adding automated restore functionality
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
- Implementing PITR restore interface
- Automated restore functionality
- Backup verification tools
- Error handling improvements
- Documentation

Please note that APIs and project structure may change if development resumes.

## License

MIT License - see LICENSE file

## Disclaimer

This software is provided as-is, without warranty. It is not production-ready and should only be used in development/testing environments. Always verify your backups and test restore procedures before relying on this tool.
