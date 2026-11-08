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
import datetime as dt
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Optional, Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from sqlalchemy import (
    JSON as SAJSON,
    Boolean as SABoolean,
    Date as SADate,
    DateTime as SADateTime,
    Enum as SAEnum,
    Float as SAFloat,
    Integer as SAInteger,
    LargeBinary as SALargeBinary,
    Numeric as SANumeric,
    String as SAString,
    Time as SATime,
    create_engine,
    inspect,
    text,
)
from sqlalchemy.engine import Engine, URL

__version__ = "1.1.0"

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
# Value normalization
#
# MySQL drivers are not always consistent about the Python type they return for
# a column: TIME columns, for instance, usually arrive as datetime.timedelta
# but PyMySQL returns the raw string for values it cannot parse. pyarrow infers
# a single type from the first values it sees, so a mixed column raises errors
# such as "object of type <class 'str'> cannot be converted to int". The
# helpers below pin one representation per column for the whole export.
# --------------------------------------------------------------------------- #
_TIME_RE = re.compile(r"^\s*([+-])?(\d+):(\d{1,2}):(\d{1,2})(?:\.(\d{1,6}))?\s*$")

POLICY_LABELS = {
    "duration": "duration",
    "datetime": "timestamp",
    "date": "date",
    "string": "text",
}


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(
        value, (str, bytes, bytearray, dt.timedelta, dt.datetime, dt.date, dt.time)
    ):
        return False
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def _category(value: object) -> str:
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "number"
    if isinstance(value, Decimal):
        return "decimal"
    if isinstance(value, dt.datetime):
        return "datetime"
    if isinstance(value, dt.date):
        return "date"
    if isinstance(value, dt.time):
        return "time"
    if isinstance(value, dt.timedelta):
        return "duration"
    if isinstance(value, (bytes, bytearray)):
        return "bytes"
    if isinstance(value, str):
        return "str"
    return "other"


def _parse_duration(value: object) -> Optional[dt.timedelta]:
    """Convert a MySQL TIME value (timedelta, time or "HH:MM:SS") to timedelta."""
    if _is_missing(value):
        return None
    if isinstance(value, dt.timedelta):
        return value
    if isinstance(value, dt.time):
        return dt.timedelta(
            hours=value.hour,
            minutes=value.minute,
            seconds=value.second,
            microseconds=value.microsecond,
        )
    if isinstance(value, str):
        match = _TIME_RE.match(value)
        if match is None:
            return None
        sign, hours, minutes, seconds, fraction = match.groups()
        parsed = dt.timedelta(
            hours=int(hours),
            minutes=int(minutes),
            seconds=int(seconds),
            microseconds=int((fraction or "0").ljust(6, "0")),
        )
        return -parsed if sign == "-" else parsed
    return None


def _format_duration(value: dt.timedelta) -> str:
    """Format a timedelta as a MySQL-like [-]HH:MM:SS[.ffffff] string."""
    negative = value < dt.timedelta(0)
    absolute = abs(value)
    total_microseconds = (
        (absolute.days * 86_400 + absolute.seconds) * 1_000_000
        + absolute.microseconds
    )
    hours, rest = divmod(total_microseconds, 3_600_000_000)
    minutes, rest = divmod(rest, 60_000_000)
    seconds, microseconds = divmod(rest, 1_000_000)

    text_value = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    if microseconds:
        text_value += f".{microseconds:06d}".rstrip("0")
    return f"-{text_value}" if negative else text_value


def _stringify(value: object) -> Optional[str]:
    """Text representation used when a column mixes incompatible types."""
    if _is_missing(value):
        return None
    if isinstance(value, dt.timedelta):
        return _format_duration(value)
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str, ensure_ascii=False)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    return str(value)


def _declared_columns(engine: Engine, table: str) -> dict[str, object]:
    """Read the column types declared in the database (best effort)."""
    schema_name, _, table_name = table.rpartition(".")
    try:
        columns = inspect(engine).get_columns(table_name, schema=schema_name or None)
    except Exception as exc:  # noqa: BLE001 - reflection is only a hint
        LOGGER.debug(
            "Could not inspect table %s (%s); inferring types from data.", table, exc
        )
        return {}
    return {column["name"]: column["type"] for column in columns}


def _arrow_type_hint(declared: object) -> Optional[pa.DataType]:
    """Best-effort Arrow type for columns that are entirely NULL in batch one."""
    if declared is None:
        return None
    if isinstance(declared, SATime):
        return pa.duration("us")
    if isinstance(declared, SADateTime):
        return pa.timestamp("us")
    if isinstance(declared, SADate):
        return pa.date32()
    if isinstance(declared, SABoolean):
        return pa.bool_()
    if isinstance(declared, SAInteger):
        return pa.int64()
    if isinstance(declared, SAFloat):
        return pa.float64()
    if isinstance(declared, SANumeric):
        precision = getattr(declared, "precision", None)
        scale = getattr(declared, "scale", None)
        if precision:
            return pa.decimal128(min(int(precision), 38), int(scale or 0))
        return pa.float64()
    if isinstance(declared, SALargeBinary):
        return pa.binary()
    if isinstance(declared, (SAString, SAJSON, SAEnum)):
        return pa.string()
    return None


def _is_temporal(declared: object) -> bool:
    return isinstance(declared, (SATime, SADateTime, SADate))


def _decide_policy(series: pd.Series, declared: object) -> str:
    """Choose one representation for a column, based on its values or DDL type."""
    if isinstance(declared, SATime):
        return "duration"
    if isinstance(declared, SADateTime):
        return "datetime"
    if isinstance(declared, SADate):
        return "date"

    values = [value for value in series.tolist() if not _is_missing(value)]
    if not values:
        return "default"

    categories = {_category(value) for value in values}
    if len(categories) == 1:
        return "string" if categories == {"other"} else "default"

    if categories <= {"duration", "time"}:
        return "duration"

    # Temporal values mixed with strings (invalid times, zero dates, ...).
    if "duration" in categories or "time" in categories:
        if all(_parse_duration(value) is not None for value in values):
            return "duration"

    return "string"


def _apply_policy(series: pd.Series, policy: str, column: str) -> pd.Series:
    before = int(series.isna().sum())

    if policy == "duration":
        converted = series.map(_parse_duration)
        introduced = int(converted.isna().sum()) - before
        if introduced > 0:
            LOGGER.warning(
                "'%s': %s value(s) could not be parsed as a time/duration "
                "and were stored as null.",
                column,
                f"{introduced:,}",
            )
        return converted

    if policy == "datetime":
        converted = pd.to_datetime(series, errors="coerce")
        introduced = int(converted.isna().sum()) - before
        if introduced > 0:
            LOGGER.warning(
                "'%s': %s invalid datetime value(s) were stored as null.",
                column,
                f"{introduced:,}",
            )
        return converted

    if policy == "date":
        parsed = pd.to_datetime(series, errors="coerce")
        introduced = int(parsed.isna().sum()) - before
        if introduced > 0:
            LOGGER.warning(
                "'%s': %s invalid date value(s) were stored as null.",
                column,
                f"{introduced:,}",
            )
        return pd.Series(
            [None if pd.isna(value) else value.date() for value in parsed],
            index=series.index,
            dtype=object,
        )

    if policy == "string":
        return series.map(_stringify)

    return series


def _normalize_batch(
    frame: pd.DataFrame,
    policies: dict[str, str],
    declared: dict[str, object],
) -> pd.DataFrame:
    """Apply the per-column policy chosen on the first batch to every batch."""
    for name in frame.columns:
        series = frame[name]
        declared_type = declared.get(name)
        is_object = series.dtype == object
        is_text = isinstance(series.dtype, pd.StringDtype)

        policy = policies.get(name)
        if policy is None:
            # Text columns without a temporal DDL type are already strings.
            if not is_object and not (is_text and _is_temporal(declared_type)):
                continue
            policy = _decide_policy(series, declared_type)
            policies[name] = policy
            if policy == "default":
                LOGGER.debug("Column '%s': keeping the inferred type.", name)
                continue
            LOGGER.info("Column '%s': exporting as %s.", name, POLICY_LABELS[policy])
        elif policy == "default":
            continue

        frame[name] = _apply_policy(series, policy, name)

    return frame


def _coerce_series(
    series: pd.Series, target: pa.DataType, column: str
) -> tuple[pa.Array, int]:
    """Convert one batch column to the pinned schema type, reporting new nulls."""
    before = int(series.isna().sum())

    if pa.types.is_string(target) or pa.types.is_large_string(target):
        values = series.map(_stringify)
        return pa.array(values, type=target), int(values.isna().sum()) - before

    if pa.types.is_timestamp(target):
        parsed = pd.to_datetime(series, errors="coerce")
        return pa.array(parsed, type=target), int(parsed.isna().sum()) - before

    if pa.types.is_date(target):
        parsed = pd.to_datetime(series, errors="coerce")
        values = [None if pd.isna(value) else value.date() for value in parsed]
        return pa.array(values, type=target), int(parsed.isna().sum()) - before

    if pa.types.is_duration(target):
        values = series.map(_parse_duration)
        return pa.array(values, type=target), int(values.isna().sum()) - before

    if pa.types.is_integer(target):
        parsed = pd.to_numeric(series, errors="coerce")
        values = [None if pd.isna(value) else int(value) for value in parsed]
        return pa.array(values, type=target), int(parsed.isna().sum()) - before

    if pa.types.is_floating(target):
        parsed = pd.to_numeric(series, errors="coerce")
        return pa.array(parsed, type=target), int(parsed.isna().sum()) - before

    if pa.types.is_binary(target):
        values = [
            None
            if _is_missing(value)
            else bytes(value)
            if isinstance(value, (bytes, bytearray))
            else str(value).encode("utf-8")
            for value in series
        ]
        return pa.array(values, type=target), sum(
            1 for value in values if value is None
        ) - before

    raise RuntimeError(
        f"Column '{column}' contains values that cannot be converted to "
        f"{target}. Cast the column explicitly in the SQL query or use a "
        "different output."
    )


def _reconcile_table(frame: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    """Build a table that fits the pinned schema, coercing mismatched columns."""
    arrays: list[pa.Array] = []
    coerced: list[tuple[str, int]] = []

    for field in schema:
        if field.name not in frame.columns:
            raise RuntimeError(f"The database batch is missing column '{field.name}'.")

        series = frame[field.name]
        try:
            arrays.append(pa.array(series, type=field.type, from_pandas=True))
            continue
        except (
            pa.ArrowInvalid,
            pa.ArrowTypeError,
            pa.ArrowNotImplementedError,
            TypeError,
        ):
            pass

        array, nulls = _coerce_series(series, field.type, field.name)
        arrays.append(array)
        if nulls > 0:
            coerced.append((field.name, nulls))

    if coerced:
        LOGGER.warning(
            "Some values did not fit the Parquet schema and were stored as null: %s",
            ", ".join(f"{name} ({count:,})" for name, count in coerced),
        )

    return pa.Table.from_arrays(arrays, schema=schema)


def _to_arrow_table(chunk: pd.DataFrame, schema: Optional[pa.Schema]) -> pa.Table:
    """Convert a pandas batch, using the schema fixed by the first batch."""
    try:
        return pa.Table.from_pandas(
            chunk, schema=schema, preserve_index=False, safe=False
        )
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError) as exc:
        if schema is None:
            raise
        LOGGER.debug("Batch does not fit the pinned schema (%s); coercing columns.", exc)
        return _reconcile_table(chunk, schema)


def _with_declared_hints(schema: pa.Schema, declared: dict[str, object]) -> pa.Schema:
    """Give all-NULL columns a real type, taken from the table definition."""
    fields: list[pa.Field] = []
    changed = False

    for field in schema:
        hint = _arrow_type_hint(declared.get(field.name))
        if pa.types.is_null(field.type) and hint is not None:
            fields.append(
                pa.field(field.name, hint, nullable=True, metadata=field.metadata)
            )
            changed = True
        else:
            fields.append(field)

    if not changed:
        return schema
    return pa.schema(fields, metadata=schema.metadata)


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
    policies: dict[str, str] = {}
    declared = _declared_columns(engine, settings.table)
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

                    chunk = _normalize_batch(chunk, policies, declared)
                    table = _to_arrow_table(chunk, schema)
                    if writer is None:
                        schema = _with_declared_hints(table.schema, declared)
                        if schema != table.schema:
                            table = table.cast(schema)
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
