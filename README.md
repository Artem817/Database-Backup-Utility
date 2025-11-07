# Database Backup Utility - Physical Backup with WAL Archiving

**Status: Development / Proof of Concept**

A database backup utility implementing native physical backup methods with WAL-based archiving for PostgreSQL and MySQL. This project is currently in active development and should be considered experimental.

## Current Implementation Status

**Working Features:**
- PostgreSQL full backup using `pg_basebackup` with WAL streaming
- PostgreSQL WAL archiving for Point-in-Time Recovery (PITR)
- MySQL full backup using `xtrabackup` (Percona XtraBackup)
- **Encrypted credential profiles (MySQL login-path, PostgreSQL .pgpass)**
- Backup catalog tracking

**Not Yet Implemented:**
- MySQL incremental backups based on LSN
- Automated restore functionality
- Point-in-Time Recovery (PITR) restore interface
- Cloud storage integration
- Backup verification
- Production-grade error handling

**Known Issues:**
- Direct filesystem access required for WAL archiving
- Limited testing across different PostgreSQL/MySQL versions
- No automated cleanup of old backups
- Chain integrity not validated
- Timezone handling inconsistencies

This utility is a work in progress and **not recommended for production use** at this time.

## Architecture

The utility uses native database backup tools instead of SQL dumps:

**PostgreSQL:**
- Full backup: `pg_basebackup` with tar format and gzip compression
- WAL archiving: Continuous archiving from `pg_wal` directory for PITR
- Requires REPLICATION privilege and `wal_level = replica`

**MySQL:**
- Full backup: `xtrabackup` physical backup with compression
- Incremental backup: Not yet implemented (planned: xtrabackup incremental based on LSN)
- Requires Percona XtraBackup installed

## Requirements

- Python 3.10 or higher
- PostgreSQL client tools (pg_basebackup)
- MySQL: Percona XtraBackup 8.0
- User permissions:
  - PostgreSQL: REPLICATION privilege
  - PostgreSQL: Read access to `pg_wal` directory for WAL archiving
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

Traditional method using `.env` file:

```bash
cp .env.example .env
```

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

Restart PostgreSQL after configuration changes.

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
- MySQL `xtrabackup` full backup (reads from `/usr/local/mysql/data/` or `/var/lib/mysql/`)
- PostgreSQL WAL archiving (reads from `/var/lib/postgresql/data/pg_wal/`)

**Operations NOT requiring sudo:**
- PostgreSQL `pg_basebackup` full backup (uses replication protocol)

### Running with elevated permissions:

**macOS/Linux:**
```bash
# MySQL full backup (requires sudo)
sudo python cli/dbtool.py backup --db mysql --database mydb --storage local --config profile

# PostgreSQL WAL archiving (requires sudo or postgres user)
sudo python cli/dbtool.py backup --db postgres --database mydb --storage local --config profile

# PostgreSQL full backup (no sudo needed - uses replication protocol)
python cli/dbtool.py backup --db postgres --database mydb --storage local --config profile
```

**Windows:**
- Run Command Prompt or PowerShell as Administrator
- Or grant read permissions to database data directories for your user account

**Alternative to sudo:**
- Run as database user: `sudo -u postgres python cli/dbtool.py ...` or `sudo -u mysql python cli/dbtool.py ...`
- Grant read permissions to data directories (one-time setup)

**Security Note:** When using `sudo` with encrypted profile configuration, credentials remain secure and are never exposed in process lists.

---

### Available commands:

```bash
# Full database backup
full database -path /path/to/backups

# WAL archiving (PostgreSQL only - for PITR)
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

Backups are organized by database name:

```
/backups/
└── DatabaseName/
    ├── full_DatabaseName_20251105_150000_a1b2/
    │   ├── base.tar.gz              # Database files
    │   ├── pg_wal.tar.gz            # WAL segments at backup time
    │   ├── backup_manifest          # PostgreSQL 13+ manifest
    │   ├── metadata.json            # Backup metadata
    │   └── wal_archives/            # WAL archiving directory
    │       ├── chain.json           # PITR restore chain
    │       ├── archive_20251105_160000_c3d4/
    │       │   ├── wal_files/
    │       │   │   └── *.gz         # Archived WAL segments
    │       │   └── metadata.json
    │       └── archive_20251105_170000_e5f6/
    │           └── ...
    └── full_DatabaseName_20251106_100000_x9y8/
        └── ...
```

Each full backup contains:
- Complete database snapshot
- WAL files at backup time
- Metadata with backup details
- WAL archives directory for continuous archiving (PITR)

## Limitations

**Current Limitations:**
- No partial/table-level backups (full database only)
- PostgreSQL WAL archiving requires direct filesystem access to `pg_wal` directory
- No automated restore scripts
- No backup validation or integrity checking
- No retention policy management
- Single-threaded operations
- Limited error recovery

**PostgreSQL:**
- WAL archiving requires running as postgres user or equivalent permissions
- WAL archiving depends on filesystem access to `pg_wal`
- PITR restore interface not yet implemented

**MySQL:**
- Only full backups currently working
- Incremental backups planned but not implemented
- Requires xtrabackup prepare step before restore (manual)

## Development Status

This project is under active development. Current focus:

- Implementing MySQL incremental backups using xtrabackup LSN-based incremental
- Adding PITR restore interface for PostgreSQL
- Improving error handling and validation
- Adding automated restore functionality
- Testing across more database versions
- Improving documentation

## Troubleshooting

**PostgreSQL WAL archiving fails with permission denied:**

WAL archiving needs read access to the PostgreSQL `pg_wal` directory. Options:
- Run as the postgres user: `sudo -u postgres python cli/dbtool.py ...`
- Run with sudo: `sudo python cli/dbtool.py ...`
- Grant read permissions to `pg_wal` directory (security consideration required)

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

This project is in early development. Contributions are welcome, particularly:

- Testing on different PostgreSQL/MySQL versions
- Implementing MySQL incremental backups
- Implementing PITR restore interface
- Automated restore functionality
- Backup verification tools
- Error handling improvements
- Documentation

Please note that the codebase is actively changing and APIs may not be stable.

## License

MIT License - see LICENSE file

## Disclaimer

This software is provided as-is, without warranty. It is not production-ready and should only be used in development/testing environments. Always verify your backups and test restore procedures before relying on this tool.