"""Cache Loader Lambda — daily data warehouse → DynamoDB + S3 overflow.

Triggered by EventBridge on a daily schedule. Extracts data from the
warehouse, transforms into hierarchical JSON, and writes to DynamoDB.
Items exceeding 400KB are stored in S3 with a DynamoDB pointer.

On failure: publishes CacheRefreshFailure metric, retains existing cache.
"""

import json
import logging
import os
import time
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CACHE_TABLE = os.environ.get("CACHE_TABLE_NAME", "ComplianceCopilot_Cache")
OVERFLOW_BUCKET = os.environ.get("OVERFLOW_BUCKET", "compliance-copilot-cache-overflow")
OVERFLOW_THRESHOLD = 400_000  # 400KB DynamoDB item limit
BATCH_SIZE = 25

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
cloudwatch = boto3.client("cloudwatch")


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for cache loading."""
    start = time.monotonic()
    logger.info("Cache loader started")

    try:
        # Import here to keep cold start fast if handler fails early
        from compliance_extractor.connection import ConnectionManager
        from compliance_extractor.join_engine import JoinEngine
        from compliance_extractor.transform import transform_rows_to_hierarchical
        from compliance_extractor.retry import execute_with_retry, RetryConfig

        # Step 1: Connect to warehouse
        mgr = ConnectionManager()
        session = mgr.connect()
        engine = JoinEngine()

        # Step 2: Extract data with retry
        config = RetryConfig(max_retries=3)
        rows = execute_with_retry(
            engine.execute_join,
            kwargs={"session": session, "timeout_seconds": 600},
            config=config,
        )
        logger.info("Extracted %d rows", len(rows))

        # Step 3: Transform to hierarchical
        hierarchical = transform_rows_to_hierarchical(rows)
        logger.info("Transformed into %d records", len(hierarchical))

        # Step 4: Write to DynamoDB with S3 overflow
        table = dynamodb.Table(CACHE_TABLE)
        written, overflowed = _write_items(table, hierarchical)

        duration = time.monotonic() - start
        logger.info(
            "Cache load complete: %d written, %d overflowed to S3 (%.1fs)",
            written, overflowed, duration,
        )

        return {
            "statusCode": 200,
            "records_written": written,
            "records_overflowed": overflowed,
            "duration_seconds": round(duration, 1),
        }

    except Exception as exc:
        logger.exception("Cache load failed")
        _publish_failure_metric()
        return {
            "statusCode": 500,
            "error": str(exc)[:500],
        }


def _write_items(table: Any, records: dict[str, dict]) -> tuple[int, int]:
    """Write records to DynamoDB, overflowing large items to S3.

    Returns:
        Tuple of (items_written_inline, items_overflowed_to_s3).
    """
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    written = 0
    overflowed = 0

    with table.batch_writer() as batch:
        for record_id, data in records.items():
            item_json = json.dumps(data, default=str)
            item_size = len(item_json.encode("utf-8"))

            if item_size > OVERFLOW_THRESHOLD:
                # Write to S3
                s3_key = f"cache/{record_id}.json"
                try:
                    s3.put_object(
                        Bucket=OVERFLOW_BUCKET,
                        Key=s3_key,
                        Body=item_json,
                        ContentType="application/json",
                    )
                    # Store pointer in DynamoDB
                    batch.put_item(Item={
                        "record_id": record_id,
                        "s3_key": s3_key,
                        "s3_overflow": True,
                        "last_updated": now,
                    })
                    overflowed += 1
                except Exception as exc:
                    # S3 failure is per-item, don't abort the batch
                    logger.error("S3 overflow failed for %s: %s", record_id, exc)
            else:
                batch.put_item(Item={
                    "record_id": record_id,
                    "data": data,
                    "last_updated": now,
                })
                written += 1

    return written, overflowed


def _publish_failure_metric() -> None:
    """Publish CacheRefreshFailure metric to CloudWatch."""
    try:
        cloudwatch.put_metric_data(
            Namespace="ComplianceCopilot",
            MetricData=[{
                "MetricName": "CacheRefreshFailure",
                "Value": 1,
                "Unit": "Count",
            }],
        )
    except Exception as exc:
        logger.error("Failed to publish failure metric: %s", exc)
