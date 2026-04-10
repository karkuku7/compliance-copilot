"""Query Lambda — API Gateway handler for compliance record lookups.

Supports:
- Exact match: GET /lookup?record_id=X → DynamoDB GetItem
- Partial match: GET /lookup?record_id=X&partial=true → DynamoDB Scan
- S3 overflow: Transparently fetches from S3 for items > 400KB
- Usage tracking: Atomic counters in a separate Usage Table
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

CACHE_TABLE = os.environ.get("CACHE_TABLE_NAME", "ComplianceCopilot_Cache")
USAGE_TABLE = os.environ.get("USAGE_TABLE_NAME", "ComplianceCopilot_Usage")
OVERFLOW_BUCKET = os.environ.get("OVERFLOW_BUCKET", "compliance-copilot-cache-overflow")

dynamodb = boto3.resource("dynamodb")
cache_table = dynamodb.Table(CACHE_TABLE)
usage_table = dynamodb.Table(USAGE_TABLE)
s3 = boto3.client("s3")


def handler(event: dict, context: Any) -> dict:
    """Lambda handler for API Gateway requests."""
    try:
        params = event.get("queryStringParameters") or {}
        record_id = params.get("record_id", "").strip()
        partial = params.get("partial", "").lower() == "true"
        path = event.get("rawPath", event.get("path", "/lookup"))

        if path.startswith("/stats"):
            return _handle_stats(params)

        if not record_id:
            return _response(400, {"error": "Missing required parameter: record_id"})

        if partial:
            return _handle_partial(record_id)
        else:
            return _handle_exact(record_id, event)

    except Exception as exc:
        logger.exception("Unhandled error")
        return _response(500, {"error": f"Internal error: {str(exc)[:200]}"})


def _handle_exact(record_id: str, event: dict) -> dict:
    """Handle exact match lookup."""
    result = cache_table.get_item(Key={"record_id": record_id})
    item = result.get("Item")

    if not item:
        return _response(404, {"error": f"Record '{record_id}' not found"})

    # Track usage (fire-and-forget)
    _record_usage(record_id, event, cache_item=item)

    # Handle S3 overflow
    if item.get("s3_overflow"):
        data = _fetch_from_s3(item["s3_key"])
        if data is None:
            return _response(500, {"error": "Failed to fetch overflow data from S3"})
        return _response(200, data)

    return _response(200, item.get("data", item))


def _handle_partial(search_term: str) -> dict:
    """Handle partial/substring match via two-phase scan."""
    # Phase 1: Lightweight scan for matching keys only
    response = cache_table.scan(ProjectionExpression="record_id")
    all_items = response.get("Items", [])

    # Handle pagination
    while "LastEvaluatedKey" in response:
        response = cache_table.scan(
            ProjectionExpression="record_id",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        all_items.extend(response.get("Items", []))

    # Filter by substring match (case-insensitive)
    search_lower = search_term.lower()
    matches = [
        item["record_id"]
        for item in all_items
        if search_lower in item["record_id"].lower()
    ]

    if not matches:
        return _response(404, {"error": f"No records matching '{search_term}'"})

    # Phase 2: Fetch full data for matches
    results = []
    for rid in matches:
        item = cache_table.get_item(Key={"record_id": rid}).get("Item")
        if item:
            if item.get("s3_overflow"):
                data = _fetch_from_s3(item["s3_key"])
                if data:
                    results.append(data)
            else:
                results.append(item.get("data", item))

    return _response(200, results)


def _handle_stats(params: dict) -> dict:
    """Handle /stats endpoint."""
    stat_type = params.get("type", "count")

    if stat_type == "count":
        response = cache_table.scan(Select="COUNT")
        return _response(200, {"count": response["Count"]})

    elif stat_type == "list":
        response = cache_table.scan(ProjectionExpression="record_id")
        names = sorted(item["record_id"] for item in response.get("Items", []))
        return _response(200, {"records": names, "count": len(names)})

    elif stat_type == "usage":
        response = usage_table.scan()
        items = response.get("Items", [])
        for item in items:
            item.setdefault("legal_reviewer_group", "")
        return _response(200, {"usage": items, "count": len(items)})

    elif stat_type == "cache-stats":
        cache_resp = cache_table.scan(ProjectionExpression="record_id, last_updated, s3_overflow")
        items = cache_resp.get("Items", [])
        overflow_count = sum(1 for i in items if i.get("s3_overflow"))
        return _response(200, {
            "total_cached": len(items),
            "overflow_items": overflow_count,
            "inline_items": len(items) - overflow_count,
        })

    elif stat_type == "reviewer-groups":
        return _stats_reviewer_groups()

    return _response(400, {"error": f"Invalid stat type: {stat_type}"})


def _stats_reviewer_groups() -> dict:
    """Return per-reviewer-group aggregated usage statistics."""
    response = usage_table.scan()
    items = response.get("Items", [])

    groups: dict[str, dict] = {}
    for item in items:
        group = item.get("legal_reviewer_group") or "Unknown"
        if not group or not group.strip():
            group = "Unknown"
        if group not in groups:
            groups[group] = {"unique_apps": 0, "total_lookups": 0}
        groups[group]["unique_apps"] += 1
        hit_count = item.get("hit_count", 0)
        groups[group]["total_lookups"] += int(hit_count)

    result = [
        {
            "reviewer_group": g,
            "unique_apps": data["unique_apps"],
            "total_lookups": data["total_lookups"],
        }
        for g, data in groups.items()
    ]
    result.sort(key=lambda x: x["total_lookups"], reverse=True)

    return _response(200, {"groups": result})


def _fetch_from_s3(s3_key: str) -> dict | None:
    """Fetch overflow data from S3."""
    try:
        response = s3.get_object(Bucket=OVERFLOW_BUCKET, Key=s3_key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception as exc:
        logger.error("S3 fetch failed for %s: %s", s3_key, exc)
        return None


def _record_usage(record_id: str, event: dict, cache_item: dict | None = None) -> None:
    """Track usage via atomic counter (fire-and-forget).

    Persists legal_reviewer_group from the cache item on every hit.
    """
    try:
        now = datetime.now(timezone.utc).isoformat()
        reviewer = (event.get("headers") or {}).get("x-reviewer-alias", "")

        # Extract legal_reviewer_group from cache item
        if cache_item is not None:
            data = cache_item.get("data", cache_item) if isinstance(cache_item, dict) else {}
            reviewer_group = data.get("app_legal_reviewer_group", "")
        else:
            reviewer_group = ""

        update_expr = (
            "ADD hit_count :one "
            "SET last_hit = :now, legal_reviewer_group = :rg"
        )
        expr_values: dict[str, Any] = {":one": 1, ":now": now, ":rg": reviewer_group}

        if reviewer:
            update_expr += ", last_reviewer = :reviewer ADD reviewers :reviewer_set"
            expr_values[":reviewer"] = reviewer
            expr_values[":reviewer_set"] = {reviewer}

        usage_table.update_item(
            Key={"record_id": record_id},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
        )
    except Exception as exc:
        # Fire-and-forget — never fail the API response
        logger.warning("Usage tracking failed: %s", exc)


def _response(status_code: int, body: Any) -> dict:
    """Build an API Gateway response."""
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
        "body": json.dumps(body, default=str),
    }
