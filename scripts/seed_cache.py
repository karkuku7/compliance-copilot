#!/usr/bin/env python3
"""Cache seeding script — populate DynamoDB from the data warehouse.

This is the primary cache population mechanism. Run it from a machine
with valid warehouse credentials (the Lambda may not have federation trust).

Usage:
    python seed_cache.py --owners alice,bob --per-table --verbose
    python seed_cache.py --record-ids App1,App2 --dry-run
    python seed_cache.py --owners alice --chunk-size 0  # adaptive chunking
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone

import boto3

# Add parent directory to path for imports
sys.path.insert(0, "..")

from compliance_extractor.connection import ConnectionManager
from compliance_extractor.constants import (
    CACHE_TABLE_NAME,
    OVERFLOW_BUCKET_NAME,
    OVERFLOW_THRESHOLD_BYTES,
)
from compliance_extractor.join_engine import JoinEngine
from compliance_extractor.retry import RetryConfig, execute_with_retry
from compliance_extractor.transform import transform_rows_to_hierarchical

logger = logging.getLogger(__name__)


# ── Adaptive chunking constants ──────────────────────────────────────
AUTO_CHUNK_APP_THRESHOLD = 200       # chunk if owner has more records than this
AUTO_CHUNK_DEFAULT_SIZE = 100        # default chunk size for large owners
AUTO_CHUNK_LARGE_OWNER_SIZE = 50     # smaller chunks for very large owners (>500 records)


def _estimate_chunk_size(app_count: int, volume_data: dict[str, int] | None = None) -> int:
    """Pick a chunk size based on the number of records and optional per-record volume data.

    When volume_data is provided, uses a memory budget of ~500K total rows
    per chunk to compute chunk size dynamically. When volume_data is None,
    falls back to existing app-count thresholds (unchanged behavior).
    """
    if volume_data is not None:
        # Volume-aware sizing: target ~500K total rows per chunk
        MEMORY_BUDGET_ROWS = 500_000
        total_volume = sum(volume_data.values())
        if total_volume > 0 and app_count > 0:
            max_volume = max(volume_data.values()) if volume_data else 0
            if max_volume > 0:
                volume_chunk = max(1, int(MEMORY_BUDGET_ROWS / max_volume))
                app_count_chunk = _estimate_chunk_size(app_count)
                if app_count_chunk == 0:
                    return volume_chunk
                return min(volume_chunk, app_count_chunk)

    # Fallback: app-count-based thresholds (original behavior)
    if app_count > 800:
        return 30
    if app_count > 500:
        return AUTO_CHUNK_LARGE_OWNER_SIZE
    if app_count > AUTO_CHUNK_APP_THRESHOLD:
        return AUTO_CHUNK_DEFAULT_SIZE
    return 0  # no chunking needed


def main():
    parser = argparse.ArgumentParser(description="Seed the compliance cache")
    parser.add_argument("--owners", help="Comma-separated owner logins")
    parser.add_argument("--record-ids", help="Comma-separated record IDs")
    parser.add_argument("--per-table", action="store_true", help="Use per-table strategy")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DynamoDB")
    parser.add_argument("--timeout", type=int, default=600, help="Query timeout (seconds)")
    parser.add_argument("--chunk-size", type=int, default=0, help="Chunk size (0=adaptive)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
    )

    owners = args.owners.split(",") if args.owners else None
    record_ids = args.record_ids.split(",") if args.record_ids else None

    # Connect
    mgr = ConnectionManager()
    session = mgr.connect()
    engine = JoinEngine()

    # Extract
    start = time.monotonic()
    config = RetryConfig(max_retries=3)

    if args.per_table and args.chunk_size >= 0 and not record_ids:
        # Use adaptive chunking for per-table strategy
        rows, total_written, total_overflowed = _run_chunked(
            args, session, engine, owners, record_ids, config,
        )
        hierarchical = transform_rows_to_hierarchical(rows)
    elif args.per_table:
        rows = execute_with_retry(
            engine.execute_per_table,
            kwargs={
                "session": session,
                "record_ids": record_ids,
                "owner_logins": owners,
                "timeout_seconds": args.timeout,
            },
            config=config,
        )
        hierarchical = transform_rows_to_hierarchical(rows)
    else:
        try:
            rows = execute_with_retry(
                engine.execute_join,
                kwargs={
                    "session": session,
                    "record_ids": record_ids,
                    "owner_logins": owners,
                    "timeout_seconds": args.timeout,
                },
                config=config,
            )
        except Exception:
            logger.warning("JOIN timed out, falling back to per-table")
            rows = execute_with_retry(
                engine.execute_per_table,
                kwargs={
                    "session": session,
                    "record_ids": record_ids,
                    "owner_logins": owners,
                    "timeout_seconds": args.timeout,
                },
                config=config,
            )
        hierarchical = transform_rows_to_hierarchical(rows)

    # Transform
    duration = time.monotonic() - start
    logger.info("Extracted %d records from %d rows in %.1fs", len(hierarchical), len(rows), duration)

    if args.dry_run:
        logger.info("Dry run — not writing to DynamoDB")
        for rid in sorted(hierarchical.keys()):
            size = len(json.dumps(hierarchical[rid]).encode("utf-8"))
            overflow = " [OVERFLOW]" if size > OVERFLOW_THRESHOLD_BYTES else ""
            logger.info("  %s (%d bytes)%s", rid, size, overflow)
        return

    # Write
    written, overflowed = _write_items(hierarchical)
    logger.info("Done: %d written, %d overflowed to S3", written, overflowed)


def _run_chunked(
    args,
    session,
    engine: JoinEngine,
    owners: list[str] | None,
    record_ids: list[str] | None,
    config: RetryConfig,
) -> tuple[list[dict], int, int]:
    """Run the seed in chunks: query records first, then process N at a time.

    Returns (all_rows, total_written, total_overflowed).
    """
    from compliance_extractor.constants import TABLE_APPLICATIONS, quote_table

    # Step 1: Query just the applications table to get the record list
    app_table = quote_table(TABLE_APPLICATIONS)
    app_query = f"SELECT DISTINCT record_id FROM {app_table}"
    if owners:
        escaped = [l.replace("'", "''") for l in owners]
        conditions = []
        for login in escaped:
            conditions.append(f"owner_login = '{login}'")
            conditions.append(f"supervisor_login = '{login}'")
            for level in range(1, 7):
                conditions.append(f"reports_to_level_{level}_login = '{login}'")
        app_query += f" WHERE ({' OR '.join(conditions)})"

    logger.info("Querying record IDs for chunking...")
    app_result = session.execute_query(app_query, timeout_seconds=args.timeout)
    if not app_result.get("success"):
        raise Exception("Record ID query failed: " + app_result.get("error_message", ""))

    all_record_ids = sorted(set(
        r["record_id"] for r in app_result.get("data", []) if r.get("record_id")
    ))
    total_records = len(all_record_ids)

    # Determine chunk size
    chunk_size = args.chunk_size
    if chunk_size == 0:
        chunk_size = _estimate_chunk_size(total_records)
    if chunk_size == 0 or chunk_size >= total_records:
        logger.info("Owner has %d records — no chunking needed", total_records)
        rows = execute_with_retry(
            engine.execute_per_table,
            kwargs={
                "session": session,
                "record_ids": record_ids,
                "owner_logins": owners,
                "timeout_seconds": args.timeout,
            },
            config=config,
        )
        return rows, 0, 0

    num_chunks = (total_records + chunk_size - 1) // chunk_size
    logger.info(
        "Adaptive chunking: %d records → %d chunks of %d",
        total_records, num_chunks, chunk_size,
    )

    all_rows: list[dict] = []
    total_written = 0
    total_overflowed = 0
    failed_chunks: list[tuple[int, list[str], str]] = []

    for i in range(0, total_records, chunk_size):
        chunk_records = all_record_ids[i : i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        logger.info(
            "Chunk %d/%d: processing %d records (%s ... %s)",
            chunk_num, num_chunks, len(chunk_records),
            chunk_records[0], chunk_records[-1],
        )

        try:
            rows = engine.execute_per_table(
                session,
                record_ids=chunk_records,
                timeout_seconds=args.timeout,
            )
            all_rows.extend(rows)
            logger.info("Chunk %d: %d rows", chunk_num, len(rows))

            # Free memory between chunks
            del rows
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as exc:
            error_msg = str(exc)
            failed_chunks.append((chunk_num, chunk_records, error_msg))
            logger.error(
                "Chunk %d/%d FAILED (%d records: %s ... %s): %s",
                chunk_num, num_chunks, len(chunk_records),
                chunk_records[0], chunk_records[-1], error_msg,
            )
            continue

    # Report failed chunks summary
    if failed_chunks:
        logger.warning(
            "Chunked processing completed with %d failed chunk(s) out of %d:",
            len(failed_chunks), num_chunks,
        )
        for chunk_num, chunk_records, error_msg in failed_chunks:
            logger.warning(
                "  Failed chunk %d: %d records (%s ... %s) — %s",
                chunk_num, len(chunk_records), chunk_records[0], chunk_records[-1], error_msg,
            )
    else:
        logger.info("All %d chunks completed successfully", num_chunks)

    return all_rows, total_written, total_overflowed


def _write_items(records: dict) -> tuple[int, int]:
    """Write records to DynamoDB with S3 overflow."""
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(CACHE_TABLE_NAME)
    s3 = boto3.client("s3")
    now = datetime.now(timezone.utc).isoformat()

    written, overflowed = 0, 0

    with table.batch_writer() as batch:
        for record_id, data in records.items():
            item_json = json.dumps(data, default=str)
            item_size = len(item_json.encode("utf-8"))

            if item_size > OVERFLOW_THRESHOLD_BYTES:
                s3_key = f"cache/{record_id}.json"
                try:
                    s3.put_object(
                        Bucket=OVERFLOW_BUCKET_NAME, Key=s3_key,
                        Body=item_json, ContentType="application/json",
                    )
                    batch.put_item(Item={
                        "record_id": record_id, "s3_key": s3_key,
                        "s3_overflow": True, "last_updated": now,
                    })
                    overflowed += 1
                    logger.info("Overflowed %s to S3 (%d bytes)", record_id, item_size)
                except Exception as exc:
                    logger.error("S3 write failed for %s: %s", record_id, exc)
            else:
                batch.put_item(Item={
                    "record_id": record_id, "data": data, "last_updated": now,
                })
                written += 1

    return written, overflowed


if __name__ == "__main__":
    main()
