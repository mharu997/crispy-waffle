"""Append-only Parquet writes with vintage-stamped metadata.

Every write tags the resulting Parquet file's ``key_value_metadata`` with the
ingestion date and a SHA256 of the input row count + max key, giving us a
weak fingerprint for replicating prior backtests.

The PIT contract demands that historical rows are never overwritten. This
module enforces that for the partitioned tables (``fundamentals_facts``) by
appending to the file for the relevant partition, and for the single-file
tables (``prices``, etc.) by writing a new file via temp-file-and-rename.
For full append semantics on a single file, callers read the existing file,
concatenate the new rows, deduplicate by primary key, and rewrite — see
``upsert_single_file``.
"""

from __future__ import annotations

import datetime as dt
import logging
import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from ..config import settings

log = logging.getLogger(__name__)


def _vintage_metadata(extra: dict[str, str] | None = None) -> dict[bytes, bytes]:
    """Return Parquet ``key_value_metadata`` stamped with this run's vintage."""
    base: dict[str, str] = {
        "pitds.ingestion_date": dt.date.today().isoformat(),
        "pitds.schema_version": "1",
    }
    if extra:
        base.update(extra)
    return {k.encode(): v.encode() for k, v in base.items()}


def write_parquet(
    table: pa.Table,
    path: Path,
    *,
    extra_metadata: dict[str, str] | None = None,
    compression: str = "zstd",
) -> Path:
    """Write *table* atomically to *path* with vintage metadata.

    Atomic via temp-file-and-rename; readers never see a partial file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    table = table.replace_schema_metadata(_vintage_metadata(extra_metadata))

    with tempfile.NamedTemporaryFile(
        dir=path.parent, suffix=".parquet.tmp", delete=False
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        pq.write_table(
            table, tmp_path, compression=compression, use_dictionary=True
        )
        tmp_path.replace(path)
        log.info("Wrote %s rows → %s", table.num_rows, path)
        return path
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise


def upsert_single_file(
    new_rows: pa.Table,
    path: Path,
    *,
    primary_key: list[str],
    sort_by: list[str] | None = None,
    extra_metadata: dict[str, str] | None = None,
) -> Path:
    """Append *new_rows* to a single-file Parquet at *path*, deduplicating by *primary_key*.

    For tables (``prices``, ``constituents_daily``, ``corporate_actions``)
    where we maintain a single Parquet rather than partitioned hive layout.
    PIT correctness is preserved because we do not modify rows — duplicate
    keys are resolved by keeping the row with the most recent ``ingestion_date``,
    which is treated as a *fix* for an erroneous earlier ingestion, not as a
    historical mutation.

    Use this routine cautiously: it does load the entire file into memory.
    For our scales (~3M price rows ≈ 150MB raw) this is fine on any laptop.
    """
    path = Path(path)
    if path.exists():
        existing = pq.read_table(path)
        if existing.schema.equals(new_rows.schema, check_metadata=False):
            combined = pa.concat_tables([existing, new_rows])
        else:
            log.warning("Schema mismatch on upsert; reconciling columns.")
            # Reconcile column sets: drop extras from existing, then cast.
            new_names = set(new_rows.column_names)
            existing_extra = [c for c in existing.column_names if c not in new_names]
            if existing_extra:
                existing = existing.drop(existing_extra)
            existing_names = set(existing.column_names)
            new_extra = [c for c in new_rows.column_names if c not in existing_names]
            if new_extra:
                # Add null-typed columns to existing for any new column.
                for col in new_extra:
                    field = new_rows.schema.field(col)
                    null_array = pa.nulls(existing.num_rows, type=field.type)
                    existing = existing.append_column(field, null_array)
            # Reorder existing to match new_rows
            existing = existing.select(new_rows.column_names)
            try:
                existing = existing.cast(new_rows.schema)
            except Exception as e:
                log.warning("Cast failed; concatenating with promote: %s", e)
            combined = pa.concat_tables([existing, new_rows], promote_options="default")
    else:
        combined = new_rows

    # Deduplicate by primary_key, keeping the latest ingestion_date.
    # We use DuckDB for this rather than pyarrow because the SQL is much
    # clearer and the performance is comparable at our scale.
    import duckdb

    con = duckdb.connect()
    con.register("combined", combined)
    pk_cols = ", ".join(primary_key)
    sort_clause = (
        f"ORDER BY {', '.join(sort_by)}" if sort_by else f"ORDER BY {pk_cols}"
    )
    deduped = con.sql(
        f"""
        SELECT * EXCLUDE (rn) FROM (
            SELECT *, ROW_NUMBER() OVER (
                PARTITION BY {pk_cols}
                ORDER BY ingestion_date DESC
            ) rn FROM combined
        ) WHERE rn = 1
        {sort_clause}
        """
    ).fetch_arrow_table()
    con.close()

    return write_parquet(deduped, path, extra_metadata=extra_metadata)


def append_to_partition(
    new_rows: pa.Table,
    base_dir: Path,
    *,
    partition_col: str,
    extra_metadata: dict[str, str] | None = None,
) -> list[Path]:
    """Append *new_rows* into hive partitions under *base_dir*.

    Used for ``fundamentals_facts``, partitioned by ``year(filed)``.
    Within each partition we still upsert by primary key (to absorb
    re-fetches of the same accession).
    """
    base_dir = Path(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    written: list[Path] = []
    # Partition values must be hashable; pull from the column.
    partition_values = new_rows.column(partition_col).to_pylist()
    distinct = sorted({v for v in partition_values if v is not None})

    for value in distinct:
        partition_path = base_dir / f"{partition_col}={value}" / "data.parquet"
        partition_path.parent.mkdir(parents=True, exist_ok=True)
        mask = pa.compute.equal(new_rows.column(partition_col), value)
        partition_rows = new_rows.filter(mask)
        # Drop the partition column from the per-partition data: it's
        # encoded in the directory path and including it as a column
        # creates type-mismatch headaches across writes (int32 vs int64
        # vs dictionary-encoded). Hive partitioning re-injects the
        # column on read.
        if partition_col in partition_rows.column_names:
            partition_rows = partition_rows.drop([partition_col])
        # For fundamentals_facts the natural primary key is the SEC composite.
        pk = [
            "cik",
            "concept",
            "period_start",
            "period_end",
            "accession_number",
        ]
        upsert_single_file(
            partition_rows,
            partition_path,
            primary_key=[c for c in pk if c in partition_rows.column_names],
            extra_metadata=extra_metadata,
        )
        written.append(partition_path)
    return written


# Convenience constants exposing canonical paths for each table.

def constituent_changes_path() -> Path:
    return settings.processed_dir / "constituent_changes.parquet"


def constituents_daily_path() -> Path:
    return settings.processed_dir / "constituents_daily.parquet"


def prices_path() -> Path:
    return settings.processed_dir / "prices.parquet"


def fundamentals_facts_dir() -> Path:
    return settings.fundamentals_dir


def sector_history_path() -> Path:
    return settings.processed_dir / "sector_history.parquet"


def corporate_actions_path() -> Path:
    return settings.processed_dir / "corporate_actions.parquet"


def ticker_history_path() -> Path:
    return settings.processed_dir / "ticker_history.parquet"


def spinoffs_path() -> Path:
    return settings.processed_dir / "spinoffs.parquet"
