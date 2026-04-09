# Architecture Deep Dive

This document captures the key design decisions, trade-offs, and technical patterns behind Compliance Copilot. It's written for engineers who want to understand *why* things are built this way — not just *what* they do.

## The Problem

Compliance teams need to review structured attestation data — records that describe how applications handle sensitive data, what controls are in place, and whether they meet regulatory requirements.

This data typically lives in a data warehouse across multiple normalized tables. Accessing it programmatically requires:

1. Complex multi-table SQL joins with deep knowledge of the schema
2. Handling of warehouse-specific data types (nested structs, arrays)
3. Deduplication across snapshot dates
4. Dealing with datasets that can exceed 100 million rows

Beyond data access, compliance reviews are manual, time-consuming, and don't scale. An experienced reviewer might spend 30-60 minutes per application. With thousands of applications, manual review is a bottleneck.

## Design Principles

1. **Cache everything, query nothing at runtime** — End users should never wait for a warehouse query
2. **No cloud credentials for end users** — The CLI and MCP tools should work with just an API URL
3. **Fail gracefully, never destructively** — Cache refresh failures retain existing data
4. **External configuration over code changes** — Prompt files, environment variables, not hardcoded values
5. **Property-based testing for data transformations** — Random inputs catch edge cases unit tests miss

---

## Component Design

### 1. Data Extraction Library

#### Dual Query Strategy

The most important architectural decision in the extraction layer. We support two query strategies:

**Strategy 1: Single SQL JOIN**
```sql
SELECT app.*, ds.*, do.*, f.*
FROM applications app
LEFT JOIN data_stores ds ON app.name = ds.application_name
LEFT JOIN data_objects do ON ds.id = do.data_store_id
LEFT JOIN object_fields f ON do.object_id = f.object_id
```

This is fast for small-to-medium datasets but times out on 100M+ rows because the warehouse's distributed query engine can't complete the four-way JOIN within timeout limits.

**Strategy 2: Per-Table Queries + Python Join**
```python
# Query each table separately
apps = query("SELECT * FROM applications WHERE owner IN (...)")
stores = query("SELECT * FROM data_stores WHERE app_name IN (...)")
objects = query("SELECT * FROM data_objects WHERE app_name IN (...)")
fields = query("SELECT * FROM object_fields WHERE app_name IN (...)")

# Join in Python using dict indexing
for app in apps:
    app['stores'] = stores_by_app.get(app['name'], [])
    for store in app['stores']:
        store['objects'] = objects_by_store.get(store['id'], [])
        # ... and so on
```

The per-table strategy is slower for small datasets but handles arbitrarily large ones because each query is scoped by the results of the previous one.

**Why not always use per-table?** Because for datasets under ~10,000 records, the single JOIN is 3-5x faster. The system tries the JOIN first and falls back to per-table on timeout.

#### ROW_NUMBER Deduplication

Warehouse tables often contain multiple snapshots of the same record. We use window functions to pick the latest:

```sql
ROW_NUMBER() OVER (
    PARTITION BY app_name, data_store_id, object_id, field_name
    ORDER BY snapshot_date DESC, snapshot_hour DESC
) AS rn
```

Only rows where `rn = 1` are kept. This runs inside the warehouse engine, so deduplication happens before data transfer.

#### Struct Parsing

Data warehouses often store nested data as string representations of structs:

```
{owner_login=alice, supervisor_login=bob, metadata={team=privacy, level=6}}
```

Our recursive parser handles:
- Nested structs (`{key={nested=value}}`)
- Arrays within structs
- NULL values and empty strings
- Escaped characters

This is one of the most bug-prone areas — property-based testing with Hypothesis generates random struct strings to verify roundtrip correctness.

#### Custom Exception Hierarchy

Every exception carries two extra attributes:

```python
class QueryTimeoutError(QueryError):
    def __init__(self, message, query_id=None):
        super().__init__(
            message,
            recoverable=True,
            suggested_action="Retry with per-table strategy or reduce dataset scope"
        )
```

This lets callers make informed decisions about retry vs. abort without parsing error messages.

### 2. Serverless Cache API

#### Why DynamoDB + S3, Not Just DynamoDB?

DynamoDB has a 400KB item size limit. Most compliance records are well under this, but some applications with hundreds of data stores and thousands of fields produce items that exceed it.

Options considered:

| Approach | Pros | Cons |
|---|---|---|
| Compress items | Simple, no extra infra | Still hits limit for very large items |
| Split across items | No extra infra | Complex reassembly, pagination |
| S3 overflow | No size limit, future-proof | Extra service, slightly higher latency |
| S3 only | Simplest | No sub-millisecond lookups |

We chose **S3 overflow** because:
1. It's future-proof — no matter how large an item gets, S3 handles it
2. The API consumer doesn't know the difference — the Query Lambda fetches from S3 transparently
3. Only ~1% of items overflow, so the common path (DynamoDB direct) stays fast

#### DynamoDB Item Structure

**Inline item** (≤ 400KB):
```json
{
    "record_id": "MyApplication",
    "last_updated": "2024-03-14T06:00:00Z",
    "data": { "...hierarchical JSON..." }
}
```

**Overflow pointer** (> 400KB):
```json
{
    "record_id": "LargeApplication",
    "last_updated": "2024-03-14T06:00:00Z",
    "s3_key": "cache/LargeApplication.json",
    "s3_overflow": true
}
```

The Query Lambda checks for `s3_overflow` and fetches from S3 when present. The API response is identical regardless of storage method.

#### Usage Tracking

Every API lookup increments an atomic counter in a separate Usage Table:

```python
table.update_item(
    Key={"record_id": record_id},
    UpdateExpression="ADD hit_count :one SET last_hit = :now",
    ExpressionAttributeValues={":one": 1, ":now": timestamp}
)
```

This is fire-and-forget — usage tracking never blocks or fails the API response. It enables:
- Understanding which records are reviewed most
- Tracking adoption over time
- Identifying records that have never been reviewed

#### Adaptive Chunking

When loading the cache, large datasets can cause Lambda OOM. The cache loader uses adaptive chunking:

```python
if total_records > 800:
    chunk_size = 30
elif total_records > 500:
    chunk_size = 50
elif total_records > 200:
    chunk_size = 100
else:
    chunk_size = total_records  # No chunking needed
```

Each chunk is processed independently — extract, transform, write — so memory usage stays bounded.

### 3. LLM Review Engine

#### Single-Pass vs. Multi-Pass

We considered two approaches for LLM review:

**Multi-pass**: Split the attestation into chunks, review each separately, merge results.
- Pro: Works within token limits
- Con: Loses cross-reference context (e.g., "Table A claims no PII but Table B references it")

**Single-pass**: Send the entire attestation in one call, let the LLM handle the full context.
- Pro: Full cross-reference capability
- Con: Requires large context window

We chose **single-pass** because compliance reviews inherently require cross-referencing across the entire attestation. The prompt instructs the LLM to structure its output even for large inputs.

#### External Prompt Files

The review prompt is loaded from a file, not hardcoded:

```python
prompt_file = os.environ.get("REVIEW_PROMPT_FILE", "prompts/default_review.md")
with open(prompt_file) as f:
    prompt_template = f.read()
```

This means:
- Different teams can customize review criteria without code changes
- Prompts can be versioned independently of the codebase
- A/B testing different prompt strategies is trivial

#### LLM via Subprocess, Not SDK

Instead of calling an LLM API directly (Bedrock, OpenAI, etc.), we pipe the prompt to a CLI tool:

```python
result = subprocess.run(
    ["llm-cli", "chat", "--no-interactive"],
    input=assembled_prompt,
    capture_output=True,
    text=True,
    timeout=review_timeout,
)
```

Why:
1. **No model invocation code to maintain** — the CLI handles auth, retries, model selection
2. **Model-agnostic** — swap the CLI tool to change models without touching review code
3. **Simpler testing** — mock the subprocess, not an SDK client
4. **No API keys in the codebase** — the CLI manages its own credentials

#### Prior Review Delta Analysis

When a prior review is provided, it's injected into the prompt:

```
--- PRIOR REVIEW ---
{prior_review_content}
--- END PRIOR REVIEW ---
Compare this attestation against the prior review and highlight what changed.
```

The LLM naturally identifies:
- New findings not in the prior review
- Resolved findings from the prior review
- Unchanged issues that still need attention

### 4. MCP Server

#### Why a Standalone MCP Server?

Options considered:

| Approach | Pros | Cons |
|---|---|---|
| Plugin to existing MCP framework | Shared infrastructure | External approval, release cycles |
| Standalone MCP server | Full control, fast iteration | Must maintain server code |
| REST API only | Simple | No agent integration |

We chose **standalone** because:
1. Full control over release cadence
2. No external approval process
3. Can add tools without coordinating with other teams
4. The MCP Python SDK makes it straightforward

#### Source Tracking

Every API call from the MCP server includes tracking headers:

```python
req.add_header("X-Source", "mcp")
req.add_header("X-Tool", tool_name)  # e.g., "lookup", "review"
```

This lets us distinguish MCP usage from CLI usage in metrics, understand which tools are most popular, and track adoption across different agent platforms.

#### Self-Contained Tools

Each MCP tool is a standalone function that takes simple parameters and returns JSON:

```python
def compliance_lookup(record_id: str, partial: bool = False) -> str:
    """Look up a compliance record by ID."""
    # ... fetch from API, return JSON
```

No shared state between tools. No database connections to manage. Each tool makes HTTP calls to the cache API and returns results. This makes testing trivial and deployment simple.

---

## Infrastructure Decisions

### Why CDK, Not Terraform/SAM?

- CDK uses the same language as the application (Python)
- Type checking catches infrastructure errors at synth time
- Constructs compose naturally (e.g., granting Lambda permissions to a table)
- The team was already proficient in Python

### Least-Privilege IAM

Each Lambda has a dedicated IAM role with only the permissions it needs:

- **Query Lambda**: DynamoDB read-only + S3 GetObject (overflow bucket only)
- **Cache Loader Lambda**: DynamoDB write + warehouse query + S3 PutObject + CloudWatch PutMetricData

We explicitly rejected using a shared admin role — it's the easy path but violates least-privilege.

### Removal Policy: RETAIN

All stateful resources (DynamoDB tables, S3 buckets) use `RemovalPolicy.RETAIN`. A `cdk destroy` won't delete your data. This is critical for a cache that takes hours to rebuild.

---

## Testing Philosophy

### Property-Based Testing for Transformations

Data transformation code is the most bug-prone part of the system. Unit tests with handcrafted inputs miss edge cases. Property-based tests with Hypothesis generate thousands of random inputs:

```python
@given(st.dictionaries(
    keys=st.text(min_size=1, max_size=20),
    values=st.one_of(st.text(), st.booleans(), st.none()),
    min_size=1,
    max_size=10,
))
def test_struct_roundtrip(data):
    """Serializing then parsing a struct should return the original data."""
    serialized = serialize_struct(data)
    parsed = parse_struct(serialized)
    assert parsed == data
```

This caught a real bug: the boolean parser only recognized `"true"` as truthy, missing `"1"` and `"yes"` — encodings used by some warehouse systems. All boolean fields were silently set to `False` across the entire cache. The LLM reviewer was producing incorrect findings as a result.

### Moto for AWS Mocking

Lambda handler tests use moto to create real (in-memory) DynamoDB tables and S3 buckets:

```python
@pytest.fixture
def dynamodb_table():
    with mock_dynamodb():
        client = boto3.resource("dynamodb", region_name="us-east-1")
        table = client.create_table(
            TableName="test-cache",
            KeySchema=[{"AttributeName": "record_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "record_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield table
```

This is faster than localstack and sufficient for testing DynamoDB/S3 interactions.

---

## Lessons Learned

### 1. Data Warehouse Federation Trust Is a Real Blocker

If your data warehouse uses federated tables (cross-account, cross-service), your Lambda execution role may not be trusted by the federation layer — even with all the right IAM permissions. The fix isn't more permissions; it's running the extraction from a trusted context (like a developer machine with proper credentials).

**Pattern**: When you can't query live from Lambda, seed a cache from a trusted environment and serve from the cache.

### 2. Boolean Encoding Is Never Simple

Different systems encode booleans differently: `true`/`false`, `1`/`0`, `yes`/`no`, `True`/`False`. If your transformation code only handles one encoding, you'll silently corrupt data. Always handle all common encodings, and use property-based tests to verify.

### 3. S3 Overflow Is Better Than Compression

When you hit DynamoDB's 400KB limit, compression seems like the obvious fix. But compression has diminishing returns on already-compact JSON, and you'll eventually hit the limit again with larger records. S3 overflow is future-proof and transparent to consumers.

### 4. LLM Reviews Need Cross-Reference Context

Splitting compliance data into chunks for separate LLM calls loses the ability to cross-reference. "Table A says no personal data, but Table B references Table A's customer_email field" — this finding requires seeing both tables in the same context window.

### 5. Usage Tracking Should Be Fire-and-Forget

Never let analytics block your API response. Use DynamoDB atomic counters with fire-and-forget writes. If the counter fails, the API still returns data. You can always backfill analytics; you can't un-fail an API call.

### 6. External Prompts Are Worth the Complexity

Hardcoding LLM prompts seems simpler, but teams inevitably want to customize review criteria. External prompt files let you iterate on prompts without deploying code, A/B test strategies, and let different teams use different criteria.
