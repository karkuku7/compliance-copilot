"""Centralized configuration constants.

All hardcoded values live here. Every constant is overridable via
environment variable for deployment flexibility.
"""

import os

# --- AWS Configuration ---
AWS_REGION = os.environ.get("CC_AWS_REGION", "us-east-1")
AWS_ACCOUNT_ID = os.environ.get("CC_AWS_ACCOUNT_ID", "123456789012")

# --- Data Warehouse ---
WAREHOUSE_DATABASE = os.environ.get("CC_WAREHOUSE_DATABASE", "compliance_db")
WAREHOUSE_WORKGROUP = os.environ.get("CC_WAREHOUSE_WORKGROUP", "primary")
WAREHOUSE_OUTPUT_LOCATION = os.environ.get(
    "CC_WAREHOUSE_OUTPUT_LOCATION",
    f"s3://compliance-copilot-query-results-{AWS_ACCOUNT_ID}/",
)
WAREHOUSE_CATALOG = os.environ.get("CC_WAREHOUSE_CATALOG", "AwsDataCatalog")

# --- Dataset Table Names ---
# Four normalized tables that form the compliance hierarchy:
#   Applications → Data Stores → Data Objects → Object Fields
TABLE_APPLICATIONS = os.environ.get("CC_TABLE_APPLICATIONS", "compliance_applications")
TABLE_DATA_STORES = os.environ.get("CC_TABLE_DATA_STORES", "compliance_data_stores")
TABLE_DATA_OBJECTS = os.environ.get("CC_TABLE_DATA_OBJECTS", "compliance_data_objects")
TABLE_OBJECT_FIELDS = os.environ.get("CC_TABLE_OBJECT_FIELDS", "compliance_object_fields")

ALL_TABLES = [TABLE_APPLICATIONS, TABLE_DATA_STORES, TABLE_DATA_OBJECTS, TABLE_OBJECT_FIELDS]


def quote_table(table_name: str) -> str:
    """Quote a table name for SQL (handles warehouse-specific quoting)."""
    return f'"{WAREHOUSE_DATABASE}"."{table_name}"'


# --- DynamoDB ---
CACHE_TABLE_NAME = os.environ.get("CC_CACHE_TABLE_NAME", "ComplianceCopilot_Cache")
USAGE_TABLE_NAME = os.environ.get("CC_USAGE_TABLE_NAME", "ComplianceCopilot_Usage")
TOOL_VERSIONS_TABLE_NAME = os.environ.get(
    "CC_TOOL_VERSIONS_TABLE_NAME", "Compliance_Tool_Versions"
)
DYNAMO_BATCH_SIZE = 25

# --- S3 Overflow ---
OVERFLOW_BUCKET_NAME = os.environ.get(
    "CC_OVERFLOW_BUCKET", "compliance-copilot-cache-overflow"
)
OVERFLOW_KEY_PREFIX = "cache/"
OVERFLOW_THRESHOLD_BYTES = 400_000  # DynamoDB 400KB item limit

# --- Lambda ---
CACHE_LOADER_LAMBDA_NAME = "ComplianceCopilot-CacheLoader"
CACHE_LOADER_MEMORY_MB = 512
CACHE_LOADER_TIMEOUT_SECONDS = 900

QUERY_LAMBDA_NAME = "ComplianceCopilot-QueryLambda"
QUERY_LAMBDA_MEMORY_MB = 256
QUERY_LAMBDA_TIMEOUT_SECONDS = 90

# --- API Gateway ---
API_GATEWAY_NAME = "ComplianceCopilot-API"
API_THROTTLE_RATE_LIMIT = 1000
API_THROTTLE_BURST_LIMIT = 2000

# --- Monitoring ---
CLOUDWATCH_NAMESPACE = "ComplianceCopilot"
CLOUDWATCH_METRIC_NAME = "CacheRefreshFailure"
CLOUDWATCH_ALARM_NAME = "ComplianceCopilot-CacheRefreshFailure"

# --- Retry ---
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_DELAY_SECONDS = 1.0
DEFAULT_BACKOFF_MULTIPLIER = 2.0

# --- Timeouts ---
CONNECTION_TIMEOUT_SECONDS = int(os.environ.get("CC_CONNECTION_TIMEOUT", "30"))
QUERY_TIMEOUT_SECONDS = int(os.environ.get("CC_QUERY_TIMEOUT", "300"))
