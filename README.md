# DB Backup Utility (PostgreSQL → CSV)

An MVP utility for:
- **Full or partial** table backups to **CSV**
- **Differential backups** based on timestamp columns (e.g., `updated_at`)
- Exporting the **schema** via `pg_dump`
- Executing **SQL** (+ optional CSV export)
- Maintaining **logs** and a **backup catalog** (`backup_catalog.json`)
- An interactive console with autocompletion (prompt-toolkit)

> **Scope (MVP):** The system supports **PostgreSQL, MySQL, and local storage**. Incremental backup is currently **unavailable**.

## Features

**Full Backup** - Complete database snapshot  
**Partial Backup** - Selected tables only  
**Differential Backup** - Only changed rows since last full backup  
**SQL Execution** - Run queries and export results  
**Compression** - Optional ZIP compression  
**Backup Catalog** - Track all backups with metadata  

## Requirements

- Python 3.10+
- `pg_dump` available in your **PATH** (PostgreSQL client tools)
- Linux/macOS/Windows  
  On Windows you may need to add: `C:\Program Files\PostgreSQL\<version>\bin` to PATH.

> **Note:** use destination **paths without spaces** for backup/export operations.

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
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=changeme
```

## Usage

### Run via .env
```bash
python3 cli/dbtool.py backup --db postgres --database mydb --storage local --config file
```
*`mydb` is the database you want to back up.*

### Run via CLI parameters
```bash
python cli/dbtool.py backup --db postgres --database mydb --storage local --config manual \
  --host localhost --port 5432 --user postgres --password changeme
```

After connecting, the interactive console opens:
```
help
full database -path /path/to/dir -compress true
full tables -tablename users -tablename orders -path /path/to/dir -compress false
differential backup
SQL SELECT * FROM users WHERE id < 100
SQL SELECT * FROM users -extract -path /tmp/exports
exit
```

#### Commands

**Full Database Backup:**
```
full database -path /path/to/backup -compress true
```

**Partial Table Backup:**
```
full tables -tablename users -tablename orders -path /path/to/backup -compress false
```

**Differential Backup:**
```
differential backup
```
> Requires a previous full backup. Exports only rows modified after the last full backup based on `updated_at` column.

**Execute SQL:**
```
SQL SELECT * FROM users WHERE id < 100
```

**Export Query Result:**
```
SQL SELECT id, email FROM users WHERE is_active = true -extract -path /tmp/exports
```

#### Flags

* `-path` — destination directory (**no spaces**)
* `-compress true|false` — also create `<backup_id>.zip` after backup
* `-tablename` — repeatable for multiple tables
* `-extract` — for `SQL ...` export the result to CSV in the given `-path`

## Output Structure

### Full/Partial Backup
```
<path>/<backup_id>/
  ├─ schema.sql
  ├─ metadata.json
  ├─ .backup_diff/              # Hidden directory for differential tracking
  │   ├─ manifest.json          # Tracks backup chain
  │   └─ <timestamp>/           # Differential backups stored here
  │       └─ <table>_diff.csv
  └─ data/
      ├─ <table1>.csv
      └─ <tableN>.csv
```

### Differential Backup
```
<last_full_backup>/.backup_diff/<timestamp>/
  ├─ <table1>_diff.csv
  └─ <tableN>_diff.csv
```

If `-compress true`, you'll also get `<path>/<backup_id>.zip`.

## Backup Catalog

All backups are tracked in `backup_catalog.json`:
```json
{
  "backups": [
    {
      "id": "full_mydb_20250101_120000_a1b2",
      "type": "full",
      "status": "completed",
      "timestamp_start": "2025-01-01T12:00:00+00:00",
      "duration_seconds": 45.2,
      "statistics": {
        "total_tables": 5,
        "total_rows_processed": 10000,
        "total_size_bytes": 5242880
      }
    }
  ]
}
```

## ⚠️ Known Issues & Limitations

This utility has some bugs that need to be fixed:

Timezone handling in differential backups (UTC vs database timezone mismatch)
Column type validation missing for differential backup basis column
Race condition when capturing timestamp for differential exports
No restore functionality for differential backups
Unlimited differential chain without automatic full backup trigger

Contributions and bug fixes are welcome!

### 📋 **Current Limitations**

* **Differential backups require:**
  - A previous full backup exists
  - Tables have `updated_at` (or similar) timestamp column
  - Column must be consistently updated on record changes

* **Not supported:**
  - Tracking deleted records (only inserts/updates)
  - Automatic restore from differential chain
  - Cloud storage (S3, GCS, etc.)

## Notes & Troubleshooting

* **Dangerous SQL** (`DROP`, `DELETE`, `TRUNCATE`, `ALTER`) require confirmation in TTY; in non-TTY mode they are skipped.
* **`pg_dump failed`** → ensure `pg_dump` is in PATH or provide a full path to it.
* **System schemas** (`pg_catalog`, `information_schema`) are not backed up.
* **Differential backup fails** → verify that tables have `updated_at` column with `timestamp` type.
* **Permission errors on `.backup_diff/`** → the utility sets secure permissions (700/600) automatically.

## Contributing

Issues and pull requests are welcome, especially for:
- Fixing the known bugs listed above
- Implementing restore functionality
- Adding column type validation
- Improving timezone handling
