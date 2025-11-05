# Database Backup Utility - WAL & Physical Backup Architecture

**Status: Development / Proof of Concept**

A database backup utility implementing native WAL-based (PostgreSQL) and physical backup (MySQL) methods. This project is currently in active development and should be considered experimental.

## Current Implementation Status

**Working Features:**
- PostgreSQL full backup using `pg_basebackup` with WAL streaming
- PostgreSQL differential backup using WAL file archiving
- MySQL full backup using `xtrabackup` (Percona XtraBackup)
- Backup catalog tracking

**Not Yet Implemented:**
- MySQL differential/incremental backups
- Automated restore functionality
- Point-in-Time Recovery (PITR)
- Cloud storage integration
- Backup verification
- Production-grade error handling

**Known Issues:**
- Permission requirements for accessing PostgreSQL data directory
- Limited testing across different PostgreSQL/MySQL versions
- No automated cleanup of old backups
- Chain integrity not validated
- Timezone handling inconsistencies

This utility is a work in progress and **not recommended for production use** at this time.

## Architecture

The utility uses native database backup tools instead of SQL dumps:

**PostgreSQL:**
- Full backup: `pg_basebackup` with tar format and gzip compression
- Differential backup: WAL file archiving from `pg_wal` directory
- Requires REPLICATION privilege and `wal_level = replica`

**MySQL:**
- Full backup: `xtrabackup` physical backup with compression
- Differential backup: Not yet implemented (planned: xtrabackup incremental)
- Requires Percona XtraBackup installed

## Requirements

- Python 3.10 or higher
- PostgreSQL client tools (pg_basebackup)
- MySQL: Percona XtraBackup 8.0
- User permissions:
  - PostgreSQL: REPLICATION privilege
  - PostgreSQL: Read access to data directory for differential backups
  - MySQL: Standard backup privileges

## Installation

```bash
git clone https://github.com/<your_user>/<repo_name>.git
cd <repo_name>
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
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

## PostgreSQL Setup

Grant replication privilege:
```sql
ALTER USER your_user REPLICATION;
```

Ensure `wal_level` is set correctly in `postgresql.conf`:
```
wal_level = replica
```

For differential backups, the utility needs read access to the PostgreSQL data directory. Run as the postgres user or grant appropriate permissions.

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

Ensure that you have the rights to back up the database. If not, you will receive a long error message stating:

```bash
xtrabackup: Can't change dir to '/usr/local/mysql/data/' (OS errno 13 - Permission denied)
```

Start the interactive console:

```bash
python cli/dbtool.py backup --db postgres --database mydb --storage local --config file
```

Available commands:

```bash
# Full database backup
full database -path /path/to/backups

# Differential backup (requires previous full backup)
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
    │   ├── pg_wal.tar.gz            # WAL segments
    │   ├── backup_manifest          # PostgreSQL 13+ manifest
    │   ├── metadata.json            # Backup metadata
    │   └── differentials/
    │       ├── chain.json           # Restore chain tracking
    │       ├── diff_20251105_160000_c3d4/
    │       │   ├── wal_archive/
    │       │   │   └── *.gz         # Compressed WAL files
    │       │   └── metadata.json
    │       └── diff_20251105_170000_e5f6/
    │           └── ...
    └── full_DatabaseName_20251106_100000_x9y8/
        └── ...
```

Each full backup contains:
- Complete database snapshot
- WAL files at backup time
- Metadata with backup details
- Differentials subdirectory for incremental WAL archives

## Restore Process (Manual)

**PostgreSQL Full Backup Restore:**

```bash
# Stop PostgreSQL
pg_ctl stop -D /var/lib/postgresql/data

# Clear data directory
rm -rf /var/lib/postgresql/data/*

# Extract base backup
tar -xzf base.tar.gz -C /var/lib/postgresql/data

# Extract WAL files
mkdir -p /var/lib/postgresql/data/pg_wal
tar -xzf pg_wal.tar.gz -C /var/lib/postgresql/data/pg_wal

# Start PostgreSQL
pg_ctl start -D /var/lib/postgresql/data
```

**PostgreSQL Differential Restore:**

After restoring the full backup, apply differential WAL files in order (as listed in chain.json):

```bash
# Decompress WAL files
cd differentials/diff_20251105_160000_c3d4/wal_archive
gunzip *.gz

# Copy to pg_wal directory
cp * /var/lib/postgresql/data/pg_wal/

# Repeat for each differential in order
# Then configure recovery and start PostgreSQL
```

Note: Automated restore is not yet implemented. This is a manual process requiring understanding of PostgreSQL recovery procedures.

## Limitations

**Current Limitations:**
- No partial/table-level backups (full database only)
- PostgreSQL differential backups require direct filesystem access to data directory
- No automated restore scripts
- No backup validation or integrity checking
- No retention policy management
- Single-threaded operations
- Limited error recovery

**PostgreSQL:**
- Differential backup requires running as postgres user or equivalent permissions
- WAL archiving depends on filesystem access
- No support for streaming replication slots

**MySQL:**
- Only full backups currently working
- Differential/incremental planned but not implemented
- Requires xtrabackup prepare step before restore (manual)

## Development Status

This project is under active development. Current focus:

- Implementing MySQL differential backups using xtrabackup incremental
- Improving error handling and validation
- Adding automated restore functionality
- Testing across more database versions
- Improving documentation

## Troubleshooting

**PostgreSQL differential backup fails with permission denied:**

The utility needs read access to the PostgreSQL data directory. Options:
- Run as the postgres user
- Grant read permissions to pg_wal directory
- Use sudo if necessary

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
- Automated restore functionality
- Backup verification tools
- Error handling improvements
- Documentation

Please note that the codebase is actively changing and APIs may not be stable.

## License

MIT License - see LICENSE file

## Disclaimer

This software is provided as-is, without warranty. It is not production-ready and should only be used in development/testing environments. Always verify your backups and test restore procedures before relying on this tool.

