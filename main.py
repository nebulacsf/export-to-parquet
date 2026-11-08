#!/usr/bin/env python3
"""Stream a large database table into a single Parquet file.

Settings are read from a ``.env`` file located next to this script (see
``.env_template``); command-line flags override the environment. The
destination path is taken from the first positional argument or, when it is
omitted, requested interactively.

Examples
--------
    python main.py /var/backups/vicidial/archive.parquet
    python main.py                       # prompts for the destination
    python main.py --dry-run
    python main.py /tmp/archive.parquet --limit 1000 --force
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL

__version__ = "1.0.0"

LOGGER = logging.getLogger("export_to_parquet")

EXIT_OK = 0
EXIT_ERROR = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130

DEFAULT_CHUNK_SIZE = 500_000
DEFAULT_COMPRESSION = "snappy"
SUPPORTED_COMPRESSIONS = ("snappy", "zstd", "gzip", "brotli", "lz4")

# Identifiers are interpolated into the SQL statement, so they are restricted
# to a conservative allow-list instead of being quoted dynamically.
TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")
ORDER_BY_RE = re.compile(
    r"^[A-Za-z0-9_.]+(?: +(?:ASC|DESC))?"
    r"(?: *, *[A-Za-z0-9_.]+(?: +(?:ASC|DESC))?)*$",
    re.IGNORECASE,
)


class ConfigError(Exception):
    """Raised when the configuration or the command-line input is invalid."""


@dataclass(frozen=True)
class Settings:
    """Resolved export configuration."""

    host: str
    port: int
    user: str
    password: str
    database: str
    table: str
    order_by: str
    chunk_size: int
    compression: Optional[str]
    output_dir: Optional[Path]
    log_level: str

    @property
    def url(self) -> URL:
        """SQLAlchemy URL; URL.create escapes special characters in the password."""
        return URL.create(
            drivername="mysql+pymysql",
            username=self.user,
            password=self.password,
            host=self.host,
            port=self.port,
            database=self.database,
        )

    @property
    def masked_url(self) -> str:
        return self.url.render_as_string(hide_password=True)

    def select_sql(self, limit: Optional[int] = None) -> str:
        sql = f"SELECT * FROM {self.table}"
        if self.order_by:
            sql += f" ORDER BY {self.order_by}"
        if limit:
            sql += f" LIMIT {int(limit)}"
        return sql

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "Settings":
        env = os.environ

        missing = [
            name
            for name in ("DB_USER", "DB_PASSWORD", "DB_NAME", "DB_TABLE")
            if env.get(name) is None
        ]
        if missing:
            raise ConfigError(
                "Missing required environment variable(s): "
                + ", ".join(missing)
                + ". Copy .env_template to .env and fill it in (`make env`)."
            )

        host = (env.get("DB_HOST") or "localhost").strip()
        port = _parse_int(env.get("DB_PORT") or "3306", "DB_PORT")
        if not 0 < port < 65536:
            raise ConfigError(f"DB_PORT is out of range: {port}.")

        table = (args.table or env.get("DB_TABLE") or "").strip()
        if not TABLE_RE.match(table):
            raise ConfigError(
                f"Invalid table name {table!r}. Use only letters, digits and "
                "underscores, with an optional schema prefix (e.g. db.table)."
            )

        order_by = (env.get("DB_ORDER_BY") or "").strip()
        if order_by and not ORDER_BY_RE.match(order_by):
            raise ConfigError(
                f"Invalid DB_ORDER_BY {order_by!r}. Expected something like "
                "'created_at ASC' or 'id DESC, created_at ASC'."
            )

        raw_chunk_size: object = (
            args.chunk_size if args.chunk_size is not None else env.get("DB_CHUNK_SIZE")
        )
        if raw_chunk_size in (None, ""):
            chunk_size = DEFAULT_CHUNK_SIZE
        else:
            chunk_size = _parse_int(raw_chunk_size, "DB_CHUNK_SIZE")
        if chunk_size <= 0:
            raise ConfigError("The batch size must be greater than zero.")

        compression = _normalize_compression(
            args.compression or env.get("PARQUET_COMPRESSION") or DEFAULT_COMPRESSION
        )

        raw_output_dir = (env.get("OUTPUT_DIR") or "").strip()
        output_dir = Path(raw_output_dir).expanduser() if raw_output_dir else None

        log_level = (args.log_level or env.get("LOG_LEVEL") or "INFO").upper()
        if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
            raise ConfigError(
                f"Invalid LOG_LEVEL {log_level!r}; use DEBUG, INFO, WARNING or ERROR."
            )

        return cls(
            host=host,
            port=port,
            user=env["DB_USER"],
            password=env.get("DB_PASSWORD", ""),
            database=env["DB_NAME"],
            table=table,
            order_by=order_by,
            chunk_size=chunk_size,
            compression=compression,
            output_dir=output_dir,
            log_level=log_level,
        )


@dataclass
class ExportResult:
    path: Path
    rows: int
    size_bytes: int
    elapsed: float


# --------------------------------------------------------------------------- #
# Configuration helpers
# --------------------------------------------------------------------------- #
def _parse_int(value: object, name: str) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{name} must be an integer, got {value!r}.") from exc


def _normalize_compression(value: str) -> Optional[str]:
    normalized = value.strip().lower()
    if normalized in ("", "none", "uncompressed"):
        return None
    if normalized not in SUPPORTED_COMPRESSIONS:
        raise ConfigError(
            f"Unsupported compression {value!r}. Choose one of: "
            + ", ".join(SUPPORTED_COMPRESSIONS)
            + ", none."
        )
    return normalized


def load_environment() -> Optional[Path]:
    """Load the .env file next to this script without overriding the shell."""
    env_file = Path(__file__).resolve().parent / ".env"
    if env_file.is_file():
        load_dotenv(env_file, override=False)
        return env_file
    LOGGER.warning(
        "No .env file found at %s; using environment variables and defaults.",
        env_file,
    )
    return None


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        stream=sys.stderr,
    )


def create_db_engine(settings: Settings) -> Engine:
    return create_engine(
        settings.url,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 30},
    )


# --------------------------------------------------------------------------- #
# Destination handling
# --------------------------------------------------------------------------- #
def prompt_output_path() -> str:
    """Ask the user where the Parquet file should be written."""
    if not sys.stdin.isatty():
        raise ConfigError(
            "No output path was provided and the session is not interactive. "
            "Pass the destination as the first argument, e.g. "
            "`python main.py /var/backups/vicidial/archive.parquet`."
        )

    print("Where should the Parquet file be saved?")
    print("Example: /var/backups/vicidial/vicidial_log_archive.parquet")
    while True:
        try:
            answer = input("Output path: ").strip()
        except EOFError as exc:
            raise ConfigError("No output path provided (end of input).") from exc
        if answer:
            return answer
        print("A destination path is required.")


def resolve_output_path(
    output: Optional[str],
    settings: Settings,
    *,
    force: bool,
    make_dirs: bool,
) -> Path:
    raw = (output or "").strip()
    if not raw:
        raw = prompt_output_path()

    path = Path(raw).expanduser()
    if not path.is_absolute():
        # A bare filename uses OUTPUT_DIR when configured; anything else is
        # resolved against the current working directory.
        base = Path.cwd()
        if settings.output_dir is not None and path.parent == Path("."):
            base = settings.output_dir
        path = base / path
    path = path.resolve()

    if path.is_dir():
        raise ConfigError(
            f"{path} is a directory; provide a full file path instead "
            f"(e.g. {path / 'export.parquet'})."
        )

    if not path.name.lower().endswith(".parquet"):
        path = path.with_name(path.name + ".parquet")
        LOGGER.info("Adding the .parquet extension: %s", path)

    parent = path.parent
    if not parent.exists():
        if not make_dirs:
            raise ConfigError(
                f"Directory {parent} does not exist "
                "(run without --no-mkdir to create it automatically)."
            )
        LOGGER.info("Creating directory %s", parent)
        parent.mkdir(parents=True, exist_ok=True)
    elif not parent.is_dir():
        raise ConfigError(f"{parent} is not a directory.")

    if path.exists():
        if force:
            LOGGER.warning("Existing file will be overwritten: %s", path)
        elif sys.stdin.isatty():
            answer = input(f"{path} already exists. Overwrite it? [y/N] ").strip().lower()
            if answer not in ("y", "yes"):
                raise ConfigError("Export cancelled; the existing file was left untouched.")
        else:
            raise ConfigError(f"{path} already exists. Re-run with --force to overwrite it.")

    return path


# --------------------------------------------------------------------------- #
# Export
# --------------------------------------------------------------------------- #
def _to_arrow_table(chunk: pd.DataFrame, schema: Optional[pa.Schema]) -> pa.Table:
    """Convert a pandas batch using the schema fixed by the first batch."""
    if schema is None:
        return pa.Table.from_pandas(chunk, preserve_index=False, safe=False)

    try:
        return pa.Table.from_pandas(
            chunk, schema=schema, preserve_index=False, safe=False
        )
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
        LOGGER.debug("Direct batch conversion failed (%s); trying an explicit cast.", exc)

    table = pa.Table.from_pandas(chunk, preserve_index=False, safe=False)
    try:
        return table.cast(schema)
    except Exception as cast_exc:  # noqa: BLE001 - surfaced with a clear message
        raise RuntimeError(
            "A batch returned by the database does not match the schema of the "
            "first batch. Try a smaller batch size or cast the conflicting "
            "columns explicitly in the SQL query."
        ) from cast_exc


def export_table(
    engine: Engine,
    settings: Settings,
    target: Path,
    *,
    limit: Optional[int] = None,
    keep_partial: bool = False,
) -> ExportResult:
    sql = settings.select_sql(limit)
    LOGGER.info("Source     : %s", settings.masked_url)
    LOGGER.info("Table      : %s", settings.table)
    LOGGER.info("Query      : %s", sql)
    LOGGER.info("Destination: %s", target)
    LOGGER.info(
        "Batch size : %s rows | compression: %s",
        f"{settings.chunk_size:,}",
        settings.compression or "none",
    )

    temp_path = target.with_name(target.name + ".part")
    if temp_path.exists():
        LOGGER.warning("Removing stale temporary file %s", temp_path)
        temp_path.unlink()

    rows_written = 0
    schema: Optional[pa.Schema] = None
    writer: Optional[pq.ParquetWriter] = None
    started = time.perf_counter()

    try:
        try:
            with engine.connect() as connection:
                batches = pd.read_sql_query(
                    sql, connection, chunksize=settings.chunk_size
                )
                for batch_number, chunk in enumerate(batches, start=1):
                    if chunk.empty:
                        continue

                    table = _to_arrow_table(chunk, schema)
                    if writer is None:
                        schema = table.schema
                        writer = pq.ParquetWriter(
                            temp_path, schema, compression=settings.compression
                        )

                    writer.write_table(table)
                    rows_written += len(chunk)

                    elapsed = time.perf_counter() - started
                    rate = rows_written / elapsed if elapsed > 0 else 0.0
                    LOGGER.info(
                        "Batch %4d | %s rows | %s total | %.0f rows/s",
                        batch_number,
                        f"{len(chunk):,}",
                        f"{rows_written:,}",
                        rate,
                    )
        finally:
            if writer is not None:
                writer.close()
                writer = None

        if rows_written == 0:
            raise RuntimeError(f"No rows were returned by `{sql}`; nothing was written.")

        os.replace(temp_path, target)
    except BaseException:
        if writer is not None:
            try:
                writer.close()
            except Exception:  # noqa: BLE001 - best effort cleanup
                pass
            writer = None
        if temp_path.exists():
            if keep_partial:
                LOGGER.warning("Partial file kept for inspection: %s", temp_path)
            else:
                temp_path.unlink(missing_ok=True)
        raise

    elapsed = time.perf_counter() - started
    return ExportResult(
        path=target,
        rows=rows_written,
        size_bytes=target.stat().st_size,
        elapsed=elapsed,
    )


def dry_run(engine: Engine, settings: Settings, limit: Optional[int]) -> None:
    """Validate the configuration and the query without writing a file."""
    sql = settings.select_sql(limit)
    LOGGER.info("Dry run: no file will be written.")
    LOGGER.info("Source: %s", settings.masked_url)
    LOGGER.info("Query : %s", sql)

    with engine.connect() as connection:
        if limit:
            LOGGER.info("Row limit: %s", f"{limit:,}")
        else:
            count = connection.execute(
                text(f"SELECT COUNT(*) FROM {settings.table}")
            ).scalar_one()
            LOGGER.info("Rows in %s: %s", settings.table, f"{count:,}")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def positive_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected an integer, got {value!r}") from exc
    if number <= 0:
        raise argparse.ArgumentTypeError("expected a value greater than zero")
    return number


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="export-to-parquet",
        description=(
            "Export a large database table to a single Parquet file, streaming "
            "the rows in batches to keep memory usage low."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "examples:\n"
            "  %(prog)s /var/backups/vicidial/archive.parquet\n"
            "  %(prog)s                        # asks for the destination path\n"
            "  %(prog)s --dry-run              # checks .env and the query only\n"
            "  %(prog)s /tmp/sample.parquet --limit 1000 --force\n"
            "  %(prog)s out.parquet --chunk-size 250000 --compression zstd\n"
        ),
    )
    parser.add_argument(
        "output",
        nargs="?",
        help="destination .parquet file; prompted for when omitted",
    )
    parser.add_argument("--table", help="override DB_TABLE")
    parser.add_argument(
        "--chunk-size",
        type=positive_int,
        help="rows per batch (overrides DB_CHUNK_SIZE)",
    )
    parser.add_argument(
        "--compression",
        help="parquet compression: snappy, zstd, gzip, brotli, lz4 or none "
        "(overrides PARQUET_COMPRESSION)",
    )
    parser.add_argument(
        "--limit",
        type=positive_int,
        help="export at most N rows (useful for smoke tests)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="overwrite the destination without asking",
    )
    parser.add_argument(
        "--no-mkdir",
        action="store_true",
        help="do not create missing parent directories",
    )
    parser.add_argument(
        "--keep-partial",
        action="store_true",
        help="keep the .part file when the export fails",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate .env, connect and show the query without writing",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="log verbosity (overrides LOG_LEVEL)",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging((args.log_level or os.getenv("LOG_LEVEL") or "INFO").upper())
    LOGGER.debug("export-to-parquet %s", __version__)

    env_file = load_environment()
    engine: Optional[Engine] = None
    try:
        settings = Settings.from_args(args)
        if env_file is not None:
            LOGGER.info("Configuration loaded from %s", env_file)

        engine = create_db_engine(settings)

        if args.dry_run:
            dry_run(engine, settings, args.limit)
            return EXIT_OK

        target = resolve_output_path(
            args.output,
            settings,
            force=args.force,
            make_dirs=not args.no_mkdir,
        )
        result = export_table(
            engine,
            settings,
            target,
            limit=args.limit,
            keep_partial=args.keep_partial,
        )

        LOGGER.info(
            "Export finished in %.1fs: %s rows, %.2f MiB -> %s",
            result.elapsed,
            f"{result.rows:,}",
            result.size_bytes / (1024 * 1024),
            result.path,
        )
        return EXIT_OK

    except ConfigError as exc:
        LOGGER.error("%s", exc)
        return EXIT_USAGE
    except KeyboardInterrupt:
        LOGGER.warning("Export interrupted; no final parquet file was produced.")
        return EXIT_INTERRUPTED
    except Exception as exc:  # noqa: BLE001 - reported to the operator
        LOGGER.error("Export failed: %s", exc)
        LOGGER.debug("Traceback:", exc_info=True)
        return EXIT_ERROR
    finally:
        if engine is not None:
            engine.dispose()


if __name__ == "__main__":
    sys.exit(main())
