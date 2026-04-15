"""Bug condition exploration tests for version-check-api-fix.

Property 1: Bug Condition - Direct API callers skip version enforcement

Direct API callers (no X-Source header, or X-Source not in ["cli", "mcp"])
should never receive a 426 Upgrade Required response, even when
X-Tool-Version is absent.
"""

import importlib
import json
import os

import boto3
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from moto import mock_aws


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

TOOL_SOURCES = {"cli", "mcp"}


def _setup_handler():
    """Reload handler module within mock_aws context so module-level
    DynamoDB resources point at mocked tables.
    """
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

    # Create cache table
    dynamodb.create_table(
        TableName="ComplianceCopilot_Cache",
        KeySchema=[{"AttributeName": "record_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "record_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    # Create usage table
    dynamodb.create_table(
        TableName="ComplianceCopilot_Usage",
        KeySchema=[{"AttributeName": "record_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "record_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    # Seed test data
    cache_table = dynamodb.Table("ComplianceCopilot_Cache")
    cache_table.put_item(Item={
        "record_id": "TestRecord",
        "data": {
            "record_id": "TestRecord",
            "description": "A test record",
            "last_updated": "2024-01-01T00:00:00Z",
            "data_stores": [],
        },
        "last_updated": "2024-01-01T00:00:00Z",
    })

    # Reload handler so module-level resources use mocked AWS
    from lambdas.query import handler as handler_module
    importlib.reload(handler_module)
    return handler_module.handler


# ---------------------------------------------------------------------------
# Hypothesis strategies
# ---------------------------------------------------------------------------

def non_tool_source_strategy():
    """Generate X-Source values that are NOT 'cli' or 'mcp'."""
    return st.one_of(
        st.just(""),
        st.just(None),
        st.text(min_size=1, max_size=30, alphabet=st.characters(categories=("L", "N", "P"))).filter(
            lambda s: s.lower() not in TOOL_SOURCES
        ),
    )


# ---------------------------------------------------------------------------
# Property 1: Bug Condition - Direct API callers skip version enforcement
# ---------------------------------------------------------------------------

@given(x_source=non_tool_source_strategy())
@settings(max_examples=50, deadline=None)
def test_direct_api_callers_skip_version_enforcement(x_source):
    """For any /lookup request where X-Source is absent or not in ["cli", "mcp"],
    the response must NOT be 426, even when X-Tool-Version is missing.
    """
    with mock_aws():
        handler = _setup_handler()

        headers = {}
        if x_source is not None:
            headers["x-source"] = x_source

        event = {
            "queryStringParameters": {"record_id": "TestRecord"},
            "headers": headers,
        }
        resp = handler(event, None)

    assert resp["statusCode"] != 426, (
        f"Direct API caller with X-Source={x_source!r} got 426"
    )
    assert resp["statusCode"] == 200


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_no_headers_at_all():
    """Empty headers dict, valid record_id -> 200."""
    with mock_aws():
        handler = _setup_handler()
        event = {
            "queryStringParameters": {"record_id": "TestRecord"},
            "headers": {},
        }
        resp = handler(event, None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["record_id"] == "TestRecord"


def test_x_source_browser():
    """X-Source: browser, no X-Tool-Version -> 200."""
    with mock_aws():
        handler = _setup_handler()
        event = {
            "queryStringParameters": {"record_id": "TestRecord"},
            "headers": {"x-source": "browser"},
        }
        resp = handler(event, None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["record_id"] == "TestRecord"


def test_empty_x_source():
    """X-Source: '', no X-Tool-Version -> 200."""
    with mock_aws():
        handler = _setup_handler()
        event = {
            "queryStringParameters": {"record_id": "TestRecord"},
            "headers": {"x-source": ""},
        }
        resp = handler(event, None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["record_id"] == "TestRecord"


def test_x_source_unknown():
    """X-Source: unknown, no X-Tool-Version -> 200."""
    with mock_aws():
        handler = _setup_handler()
        event = {
            "queryStringParameters": {"record_id": "TestRecord"},
            "headers": {"x-source": "unknown"},
        }
        resp = handler(event, None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["record_id"] == "TestRecord"
