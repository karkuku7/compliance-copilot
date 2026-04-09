#!/usr/bin/env python3
"""Cache seeding script — populate DynamoDB from the data warehouse.

This is the primary cache population mechanism. Run it from a machine
with valid warehouse credentials (the Lambda may not have federation trust).

Usage:
    python seed_cache.py --owners alice,bob --per-table --verbose
    python seed_cache.py --record-ids App1,App2 --dry-run
    python seed_cache.py --owners alice --chunk-size 0  # adaptive chunking
"""

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

    if args.per_table:
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

    # Transform
    hierarchical = transform_rows_to_hierarchical(rows)
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
