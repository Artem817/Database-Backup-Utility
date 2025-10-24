# DB Backup Utility (PostgreSQL → CSV)

An MVP utility for:
- Full or partial table backups to **CSV**
- Exporting the **schema** via `pg_dump`
- Executing **SQL** (+ optional CSV export)
- Maintaining **logs** and a **backup catalog** (`backup_catalog.json`)
- An interactive console with autocompletion (prompt-toolkit)

> **Scope (MVP):** supports **PostgreSQL only** and **local** storage. Differential/incremental backups are not implemented yet.

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
````

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
python dbtool.py backup --db postgres --database mydb --storage local --config file
```

*`mydb` is the database you want to back up.*

### Run via CLI parameters

```bash
python dbtool.py backup --db postgres --database mydb --storage local --config manual \
  --host localhost --port 5432 --user postgres --password changeme
```

After connecting, the interactive console opens:

```
help
full database -path /path/to/dir -compress true
full tables -tablename users -tablename orders -path /path/to/dir -compress false
SQL SELECT * FROM users WHERE id < 100
SQL SELECT * FROM users -extract -path /tmp/exports
exit
```

#### Flags

* `-path` — destination directory (**no spaces**)
* `-compress true|false` — also create `<backup_id>.zip` after backup
* `-tablename` — repeatable for multiple tables
* `-extract` — for `SQL ...` export the result to CSV in the given `-path`

### Additional examples

**Partial backup of specific tables:**

```bash
full tables -tablename users -tablename orders -path /tmp/db_backups -compress true
```

**Export query result to CSV:**

```bash
SQL SELECT id, email FROM users WHERE is_active = true -extract -path /tmp/query_exports
```

## Output Structure

```
<path>/<backup_id>/
  ├─ schema.sql
  ├─ metadata.json
  └─ data/
      ├─ <table1>.csv
      └─ <tableN>.csv
```

If `-compress true`, you’ll also get `<path>/<backup_id>.zip`.

## Notes & Troubleshooting

* **Dangerous SQL** (`DROP`, `DELETE`, `TRUNCATE`, `ALTER`) require confirmation in TTY; in non-TTY mode they are skipped.
* `pg_dump failed` → ensure `pg_dump` is in PATH or provide a full path to it.
* System schemas (`pg_catalog`, `information_schema`) are not backed up.
