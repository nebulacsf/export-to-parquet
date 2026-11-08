# export-to-parquet

Export a large MySQL/MariaDB table to a single Parquet file without loading the
whole table into memory. Rows are read in batches with `pandas`, converted with
`pyarrow` and appended to one Parquet file.

Designed for archive/backup tables with millions of rows (for example
`vicidial_log_archive`).

## How it works

1. `main.py` loads the connection settings from `.env` (created from
   `.env_template`).
2. The destination path comes from the first positional argument or from an
   interactive prompt when no argument is given. Missing parent directories are
   created automatically.
3. The table is streamed with `SELECT * FROM <table> [ORDER BY ...]` using
   batches of `DB_CHUNK_SIZE` rows (500 000 by default).
4. The first batch fixes the Parquet schema; the remaining batches are written
   against it, so memory usage stays flat regardless of table size.
5. Data is written to `<destination>.part` and renamed to the final name only
   after the export succeeds, so an interrupted run never leaves a truncated
   file that looks valid.
6. Column types are pinned once and reused for every batch. `TIME` columns are
   exported as durations, dates/timestamps keep their temporal type, and
   columns that mix incompatible Python types are exported as text instead of
   aborting the export.
7. A summary is printed: rows, elapsed time, file size and final path.

## Requirements

- Python 3.10 or newer
- Network access to the MySQL/MariaDB server
- A database user with `SELECT` permission on the source table
- Write permission on the destination directory
- Enough free disk space: Parquet is usually much smaller than the source
  table, but the temporary `.part` file exists while the export runs

## Quick start

```bash
make setup      # create .venv and install requirements.txt
make env        # copy .env_template to .env
$EDITOR .env    # set DB_USER, DB_PASSWORD, DB_NAME, DB_TABLE
make check      # validate configuration, connection and query
make run        # export; the script asks where to save the file
```

Non-interactive usage (scripts, cron):

```bash
make export OUT=/var/backups/vicidial/vicidial_log_archive.parquet
```

## Configuration (`.env`)

| Variable             | Default     | Description |
|----------------------|-------------|-------------|
| `DB_HOST`            | `localhost` | Database host. |
| `DB_PORT`            | `3306`      | Database port. |
| `DB_USER`            | —           | Database user (required). |
| `DB_PASSWORD`        | —           | Database password (required, may be empty). |
| `DB_NAME`            | —           | Database/schema name (required). |
| `DB_TABLE`           | —           | Source table, optionally `schema.table` (required). |
| `DB_ORDER_BY`        | empty       | `ORDER BY` clause, e.g. `call_date ASC`. Empty means no sorting. |
| `DB_CHUNK_SIZE`      | `500000`    | Rows read per batch. |
| `PARQUET_COMPRESSION`| `snappy`    | `snappy`, `zstd`, `gzip`, `brotli`, `lz4` or `none`. |
| `OUTPUT_DIR`         | empty       | Directory used when a bare filename is given instead of a full path. |
| `LOG_LEVEL`          | `INFO`      | `DEBUG`, `INFO`, `WARNING` or `ERROR`. |

Command-line flags always win over `.env` values. The `.env` file is listed in
`.gitignore`; never commit real credentials.

### About `DB_ORDER_BY`

Parquet readers do not need sorted data, so the `ORDER BY` is optional. On very
large tables MySQL may have to sort millions of rows to disk, which can take
longer than the export itself. If the export is slow at the "connecting" stage,
set `DB_ORDER_BY=` (empty) and let the database return rows in storage order.

## Command-line options

```text
usage: export-to-parquet [-h] [--table TABLE] [--chunk-size CHUNK_SIZE]
                         [--compression COMPRESSION] [--limit LIMIT] [--force]
                         [--no-mkdir] [--keep-partial] [--dry-run]
                         [--log-level {DEBUG,INFO,WARNING,ERROR}] [--version]
                         [output]

positional arguments:
  output                destination .parquet file; prompted for when omitted

options:
  --table TABLE         override DB_TABLE
  --chunk-size N        rows per batch (overrides DB_CHUNK_SIZE)
  --compression NAME    snappy, zstd, gzip, brotli, lz4 or none
  --limit N             export at most N rows (useful for smoke tests)
  --force               overwrite the destination without asking
  --no-mkdir            do not create missing parent directories
  --keep-partial        keep the .part file when the export fails
  --dry-run             validate .env, connect and show the query without writing
  --log-level LEVEL     log verbosity
  --version             show the script version
```

Behavior notes:

- Without a positional argument the script asks: `Output path:`.
- A relative path with a directory part (`./exports/db.parquet`) is resolved
  against the current directory; a bare filename (`db.parquet`) is resolved
  against `OUTPUT_DIR` when that variable is set.
- The destination name must end in `.parquet`; the extension is appended when
  it is missing.
- If the file already exists, the script asks for confirmation; in a
  non-interactive session it fails unless `--force` is used.
- Exit codes: `0` success, `1` runtime error, `2` configuration/usage error,
  `130` interrupted with Ctrl+C.

## Make targets

| Target | Description |
|--------|-------------|
| `make` / `make help` | List all targets, variables and examples. |
| `make setup` | Create `.venv` and install `requirements.txt`. |
| `make install` | Reinstall dependencies in the existing environment. |
| `make env` | Copy `.env_template` to `.env` (never overwrites an existing file). |
| `make check` | Dry run: validate `.env`, dependencies, connection and query. |
| `make doctor` | Print Python and dependency versions, and whether `.env` exists. |
| `make run` | Interactive export; prompts for the destination path. |
| `make export OUT=...` | Non-interactive export, overwrites the destination. |
| `make sample OUT=... [LIMIT=n]` | Test export limited to 1000 rows by default. |
| `make clean` | Remove `__pycache__` and stale `.part` files. |
| `make distclean` | `clean` plus remove `.venv` (`.env` and exports are kept). |

Per-run overrides:

```bash
make export OUT=/tmp/archive.parquet TABLE=other_table CHUNK_SIZE=250000
make export OUT=/tmp/archive.parquet COMPRESSION=zstd
make sample OUT=/tmp/archive_sample.parquet LIMIT=5000
```

## Scheduling with cron

```cron
# every day at 03:00
0 3 * * * cd /opt/export-to-parquet && /usr/bin/make export \
    OUT=/var/backups/vicidial/vicidial_log_archive.parquet \
    >> /var/log/export-to-parquet.log 2>&1
```

`make export` passes `--force`, so the previous file is replaced only after the
new export completes successfully.

## Performance tips

- **RAM:** peak memory is roughly one batch, not the whole table. Lower
  `DB_CHUNK_SIZE` (e.g. `100000`) on small servers.
- **Speed:** increase `DB_CHUNK_SIZE` on servers with plenty of RAM.
- **Size:** `zstd` produces smaller files than `snappy` at the cost of CPU
  time; `snappy` is the best default for backups.
- **Network:** run the script on the database host (as the original
  `/var/backups/vicidial` path suggests) to avoid transferring the whole table
  twice.
- **Indexes:** with `DB_ORDER_BY` set, an index on that column makes the read
  sequential instead of a full filesort.

## Column type handling

MySQL drivers can return more than one Python type for the same column. A
`TIME` column, for example, usually arrives as `datetime.timedelta`, but
PyMySQL returns the raw string for values it cannot parse; pyarrow then fails
with errors like `object of type <class 'str'> cannot be converted to int`.

The script normalizes each column once, on the first batch, and applies the
same representation to every subsequent batch:

| Database type | Parquet representation |
|---------------|------------------------|
| `TIME` | `duration[us]` (values like `"25:00:00"` are parsed, including negative and >24 h durations). |
| `DATE`, `DATETIME`, `TIMESTAMP` | `date32` / `timestamp[us]`. |
| `DECIMAL` | `decimal128(precision, scale)`. |
| Text, JSON, enums | UTF-8 text (JSON objects are serialized with `json.dumps`). |
| Columns mixing incompatible types | Text, preserving every value as a string. |
| Columns that are entirely `NULL` in the first batch | Type taken from the table definition, so later batches can still be written. |

Unparseable temporal values (for example MySQL zero dates `0000-00-00` or a
malformed `TIME` string) are stored as `NULL` and reported in a warning. The
export does not stop, and every other row is preserved. This is the same
behavior as pandas' `errors="coerce"`.

## Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `Missing required environment variable(s)` | `.env` is incomplete: run `make env` and fill it in. |
| `Missing .venv. Run "make setup" first.` | Dependencies are not installed: run `make setup`. |
| `No .env file found` warning | Running outside the project or `.env` was not created; `make env`. |
| `Access denied for user ...` | Wrong `DB_USER`/`DB_PASSWORD`, or the user lacks `SELECT` on the table. |
| `RuntimeError: 'cryptography' package is required` | MySQL 8 authentication plugin; `cryptography` is already in `requirements.txt`, rerun `make install`. |
| `Permission denied` on `/var/backups/...` | The user cannot write there: use an `OUT` path you own, or adjust ownership/permissions on the directory. |
| `No rows were returned` | Table is empty or `DB_TABLE` points to the wrong table. |
| `object of type <class 'str'> cannot be converted to int` on a column | The driver returned mixed types for that column (common with `TIME` values). Fixed in v1.1.0: the column is normalized to one type for the whole export. |
| `does not match the schema of the first batch` | Rows change type between batches; the script now coerces mismatched values, but lower `--chunk-size` or cast the column in the SQL query if it persists. |
| Warnings about values `stored as null` | A `TIME`, `DATE` or `DATETIME` value could not be parsed (e.g. MySQL zero dates `0000-00-00`). The rest of the export continues; see below. |
| Export seems stuck before the first batch | `ORDER BY` sorting a huge table; set `DB_ORDER_BY=` to skip it. |
| `Incorrect datetime value: '0000-00-00'` | Legacy zero dates are not parseable; fix the data or filter those rows in the SQL query. |
| `No space left on device` | Free space in the destination filesystem; the temporary `.part` file is deleted on failure. |
| Interrupted with Ctrl+C | No final file is produced; the `.part` file is removed. Use `--keep-partial` to inspect it. |

## Project layout

```text
.
├── main.py           # export script (CLI)
├── .env_template     # configuration template → copy to .env
├── requirements.txt  # Python dependencies
├── Makefile          # one command per task
├── README.md
└── .gitignore
```
