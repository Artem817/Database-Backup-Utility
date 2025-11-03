# DB Backup Utility (PostgreSQL & MySQL → ZSTD)

An MVP utility for creating efficient database backups using native database utilities and modern compression:
- **Full or partial** database backups to **compressed SQL** (.sql.zst)
- **Differential backups** based on timestamp columns (e.g., `updated_at`)
- **Native database utilities** (`pg_dump`, `mysqldump`) for optimal performance
- **ZSTD compression** for superior compression ratios
- **Backup catalog** tracking with detailed metadata
- Interactive console with autocompletion (prompt-toolkit)

> **Scope (MVP):** Supports **PostgreSQL, MySQL** with **local storage**. Uses native database tools for reliability and performance.

## Features

**Full Backup** - Complete database snapshot using native utilities  
**Partial Backup** - Selected tables only with zstd compression  
**Differential Backup** - Only changed rows since last full backup  
**SQL Execution** - Run queries and export results to CSV  
**ZSTD Compression** - Superior compression for all backup types  
**Backup Catalog** - Track all backups with detailed metadata  

## Requirements

- Python 3.10+
- **PostgreSQL**: `pg_dump` available in PATH
- **MySQL**: `mysqldump` available in PATH  
- **ZSTD**: `zstd` command-line tool installed
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

## Technical Details

### Backup Methods

**PostgreSQL:**
- **Full**: `pg_dump -Fc database | zstd → database.sql.zst`
- **Partial**: `pg_dump -Fc -t table1 -t table2 | zstd → database_partial.sql.zst`
- **Differential**: `pg_dump --data-only --where="updated_at > timestamp" | zstd`

**MySQL:**
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

**Common Issues:**

* **Dangerous SQL** (`DROP`, `DELETE`, `TRUNCATE`, `ALTER`) require confirmation in TTY; in non-TTY mode they are skipped.
* **`pg_dump failed`** → Ensure PostgreSQL client tools are installed and in PATH
* **`mysqldump failed`** → Ensure MySQL client tools are installed and in PATH  
* **`zstd command not found`** → Install zstd compression tool
* **System schemas** (`pg_catalog`, `information_schema`) are not backed up.
* **Differential backup fails** → Verify tables have `updated_at` timestamp column
* **Permission errors on `.backup_diff/`** → The utility sets secure permissions (700/600) automatically.

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

