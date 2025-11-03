# DB Backup Utility (PostgreSQL & MySQL → ZSTD)

An MVP utility for creating efficient database backups using native database utilities:
- **PostgreSQL**: WAL-based backups with `pg_basebackup` (binary format)
- **MySQL**: Traditional backups with `mysqldump` (SQL dumps)
- **Full or partial** database backups with native compression
- **Differential backups** based on timestamp columns (MVP approach)
- **SQL execution** with optional CSV export
- **Backup catalog** tracking with detailed metadata
- Interactive console with autocompletion (prompt-toolkit)

> **Scope (MVP):** Supports **PostgreSQL, MySQL** with **local storage**. PostgreSQL uses WAL-based architecture, MySQL uses traditional dumps.

## Features

**🗄️ Full Backup** 
- **PostgreSQL**: Binary backup with pg_basebackup + WAL streaming
- **MySQL**: SQL dump with mysqldump + zstd compression

**Partial Backup** - Selected tables only (both databases)  
**Differential Backup** - Only changed rows since last full backup (MVP: updated_at based)  
**SQL Execution** - Run queries and export results to CSV  
**Native Compression** - Built-in compression for optimal performance  
**Backup Catalog** - Track all backups with detailed metadata  
**Backup Catalog** - Track all backups with detailed metadata  

## Requirements

- Python 3.10+
- **PostgreSQL**: 
  - `pg_basebackup` available in PATH
  - User must have REPLICATION privilege
  - PostgreSQL 10+ recommended
- **MySQL**: 
  - `mysqldump` available in PATH
  - Standard user privileges sufficient
- Linux/macOS/Windows
  
> **Note:** Use destination **paths without spaces** for backup operations.

## Installation

```bash
git clone https://github.com/<your_user>/<repo_name>.git
cd <repo_name>
python -m venv .venv

# Linux/macOS:
source .venv/bin/activate

# Windows:
# .venv\Scripts\activate

pip install -r requirements.txt
cp .env.example .env
```

Fill in `.env`:
```dotenv
# PostgreSQL example
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=changeme
DB_NAME=mydb

# MySQL example  
# DB_HOST=localhost
# DB_PORT=3306
# DB_USER=root
# DB_PASSWORD=changeme
# DB_NAME=mydb
```

## Usage

### Run via .env
```bash
python3 cli/dbtool.py backup --db postgres --database mydb --storage local --config file
```

### Run via CLI parameters
```bash
python cli/dbtool.py backup --db postgres --database mydb --storage local --config manual \
  --host localhost --port 5432 --user postgres --password changeme
```

After connecting, the interactive console opens:
```
[mydb]> help
[mydb]> full database -path /path/to/backups
[mydb]> full tables -tablename users -tablename orders -path /path/to/backups  
[mydb]> differential backup
[mydb]> SQL SELECT * FROM users WHERE id < 100
[mydb]> SQL SELECT * FROM users -extract -path /tmp/exports
[mydb]> exit
```

## Commands

### Full Database Backup
```bash
full database -path /path/to/backups
```
Creates: `database_20251101_120000.sql.zst`

### Partial Table Backup  
```bash
full tables -tablename users -tablename orders -path /path/to/backups
```
Creates: `database_partial_20251101_120000.sql.zst`

### Differential Backup
```bash
differential backup
```
Creates: `database_diff_20251101_120000.sql.zst`

> **Requires:** Previous full backup and tables with `updated_at` timestamp column.

### Execute SQL
```bash
SQL SELECT * FROM users WHERE id < 100
```

### Export Query Results
```bash
SQL SELECT id, email FROM users WHERE is_active = true -extract -path /tmp/exports
```
> **Requires:** Previous full backup and tables with `updated_at` timestamp column.
> **Note:** This is MVP approach; WAL-based incremental backup is planned.

## Technical Details

### Backup Methods

**PostgreSQL (WAL-based):**
- **Full**: `pg_basebackup -F t -X stream --checkpoint=fast`
  - Binary tar format with WAL streaming
  - ACID-compliant consistent snapshot
  - Native compression support
  - Requires REPLICATION privilege
- **Partial**: `pg_dump -Fc -t table1 -t table2 | zstd`
- **Differential**: `pg_dump --data-only --where="updated_at > timestamp" | zstd`
  - ⚠️ MVP approach, not WAL-based (planned for future)

**MySQL (Traditional):**
- **Full**: `mysqldump --single-transaction database | zstd → database.sql.zst`  
- **Partial**: `mysqldump --single-transaction database table1 table2 | zstd`
- **Differential**: `mysqldump --no-create-info --where="updated_at > timestamp" | zstd`

### Compression Benefits

**ZSTD vs traditional methods:**
- **~40% better compression** than gzip
- **~60% better compression** than ZIP
- **Faster decompression** than most alternatives
- **Single file** instead of multiple CSV files

## Output Structure

### Full/Partial Backup
```
<path>/<backup_id>/
  ├─ <database>_20251101_120000.sql.zst     # Main backup file
  ├─ schema.sql                             # Database schema
  ├─ metadata.json                          # Backup metadata
  └─ .backup_diff/                          # Hidden directory for differential tracking
      ├─ manifest.json                      # Tracks backup chain
      └─ <timestamp>/                       # Differential backups
          └─ <database>_diff_<timestamp>.sql.zst
```

### File Examples
- **Full**: `mydb_20251101_120000.sql.zst`
- **Partial**: `mydb_partial_20251101_120000.sql.zst`  
- **Differential**: `mydb_diff_20251101_130000.sql.zst`

## Backup Catalog

All backups are tracked in `backup_catalog.json`:
```json
{
  "backups": [
    {
      "id": "full_mydb_20251101_120000_a1b2",
      "type": "full", 
      "status": "completed",
      "timestamp_start": "2025-11-01T12:00:00+00:00",
      "duration_seconds": 12.5,
      "compress": true,
      "backup_location": "/backups/full_mydb_20251101_120000_a1b2",
      "statistics": {
        "total_tables": 5,
        "total_rows_processed": 50000,
        "total_size_bytes": 2097152
      }
    }
  ]
}
```

## ⚠️ Known Issues & Limitations

### 🐛 **Known Bugs**

This utility has some bugs that need to be fixed:

- **Timezone handling** in differential backups (UTC vs database timezone mismatch)
- **Column type validation** missing for differential backup basis column
- **Race condition** when capturing timestamp for differential exports
- **No restore functionality** for differential backups
- **Unlimited differential chain** without automatic full backup trigger

Contributions and bug fixes are welcome!

### 📋 **Current Limitations**

**PostgreSQL:**
- **Full backup**: Production-ready WAL-based
- **Differential**: MVP approach using `updated_at` (not WAL-based)
- **No PITR** yet (foundation exists, implementation planned)
- **No WAL archiving** for continuous backup

**Not supported:**
- Tracking deleted records (only inserts/updates)
- Automatic restore from differential backup chain
- Cloud storage integration (S3, GCS, etc.)
- Incremental backups (planned for future versions)

## Troubleshooting

**PostgreSQL specific:**
* **`FATAL: must be superuser or replication role`** → Grant REPLICATION privilege:
  ```sql
  ALTER USER your_user REPLICATION;
  ```
* **`pg_basebackup: command not found`** → Install PostgreSQL client tools

**MySQL specific:**
* **`mysqldump failed`** → Ensure MySQL client tools are installed
* **Permission denied** → Check user has SELECT privilege on all tables

**Common issues:**
* **Dangerous SQL** (`DROP`, `DELETE`) require confirmation in interactive mode
* **System schemas** (`pg_catalog`, `information_schema`) are not backed up
* **Differential backup fails** → Verify tables have `updated_at` timestamp column

## Performance Tips

- **Use SSD storage** for backup destinations
- **ZSTD compression** is CPU-intensive but provides excellent ratios
- **Differential backups** are much faster for frequently changing data
- **Partial backups** for specific table subsets reduce backup time

## Contributing

Issues and pull requests are welcome, especially for:
- Fixing the known bugs listed above
- Implementing automated restore functionality  
- Adding column type validation
- Improving timezone handling
- Cloud storage integration
- Backup verification and integrity checks

