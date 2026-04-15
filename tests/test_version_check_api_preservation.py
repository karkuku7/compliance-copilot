"""Preservation tests for version-check-api-fix.

Property 2: Preservation - Tool client version enforcement unchanged

Known tool clients (X-Source in ["cli", "mcp"]) without X-Tool-Version
must still receive a 426 Upgrade Required response. Other behaviors
(200, 400, 404) must remain unchanged.
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

TOOL_SOURCES = ("cli", "mcp")


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

def tool_source_strategy():
    """Generate X-Source values that ARE known tool clients: 'cli' or 'mcp'."""
    return st.sampled_from(["cli", "mcp"])


# ---------------------------------------------------------------------------
# Property 2: Preservation - Tool client version enforcement unchanged
# ---------------------------------------------------------------------------

@given(x_source=tool_source_strategy())
@settings(max_examples=50, deadline=None)
def test_tool_clients_without_version_get_426(x_source):
    """For any /lookup request where X-Source is in ["cli", "mcp"] and
    X-Tool-Version is absent, the response must be 426 Upgrade Required.
    """
    with mock_aws():
        handler = _setup_handler()

        event = {
            "queryStringParameters": {"record_id": "TestRecord"},
            "headers": {"x-source": x_source},
        }
        resp = handler(event, None)

    assert resp["statusCode"] == 426, (
        f"Tool client with X-Source={x_source!r} and no X-Tool-Version "
        f"should get 426, got {resp['statusCode']}"
    )
    body = json.loads(resp["body"])
    assert body["error"] == "UpgradeRequired"


# ---------------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------------

def test_cli_with_version_returns_200():
    """X-Source: cli, X-Tool-Version: 0.2.0, valid record_id -> 200."""
    with mock_aws():
        handler = _setup_handler()
        event = {
            "queryStringParameters": {"record_id": "TestRecord"},
            "headers": {
                "x-source": "cli",
                "x-tool-version": "0.2.0",
            },
        }
        resp = handler(event, None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["record_id"] == "TestRecord"


def test_mcp_without_version_returns_426():
    """X-Source: mcp, no X-Tool-Version -> 426."""
    with mock_aws():
        handler = _setup_handler()
        event = {
            "queryStringParameters": {"record_id": "TestRecord"},
            "headers": {"x-source": "mcp"},
        }
        resp = handler(event, None)
    assert resp["statusCode"] == 426
    body = json.loads(resp["body"])
    assert body["error"] == "UpgradeRequired"


def test_missing_record_id_returns_400():
    """No record_id -> 400."""
    with mock_aws():
        handler = _setup_handler()
        event = {
            "queryStringParameters": {},
            "headers": {
                "x-source": "cli",
                "x-tool-version": "0.2.0",
            },
        }
        resp = handler(event, None)
    assert resp["statusCode"] == 400


def test_unknown_record_returns_404():
    """Valid headers, record_id=NonExistent -> 404."""
    with mock_aws():
        handler = _setup_handler()
        event = {
            "queryStringParameters": {"record_id": "NonExistent"},
            "headers": {
                "x-source": "cli",
                "x-tool-version": "0.2.0",
            },
        }
        resp = handler(event, None)
    assert resp["statusCode"] == 404
