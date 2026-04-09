# Technical Roadblocks & Solutions

Real challenges encountered while building this system, and how they were resolved. These aren't theoretical — each one blocked progress for hours or days.

---

## 1. Data Warehouse Federation Trust Barrier

### The Problem

The cache loader Lambda needed to query a data warehouse (Athena) that uses federated tables. Federated tables resolve their metadata through a trust chain — the calling principal must be trusted by the federation layer.

The Lambda execution role had:
- ✅ Lake Formation SELECT grants on all four tables
- ✅ Glue GetTable, GetDatabase permissions
- ✅ Athena StartQueryExecution, GetQueryResults
- ✅ S3 read/write for query results

But every query failed with: `StorageDescriptor is null`

This means the federation layer couldn't resolve the table's physical storage location for the Lambda's IAM role.

### What We Tried (and Failed)

1. **More IAM permissions** — Added every Glue, Lake Formation, and Athena permission. No effect.
2. **Lake Formation admin** — Temporarily made the Lambda role an LF admin. Still failed.
3. **Cross-account Glue catalog** — Tried querying via the source account's catalog directly. Same error.
4. **Cross-catalog queries** — Used Athena's cross-catalog syntax. Federation still blocked it.
5. **Assumed role with broader trust** — Created a role trusted by the federation service. The federation trust is per-principal, not per-role.

### The Solution

**Seed the cache from a trusted environment instead of querying live from Lambda.**

A developer machine (or CI/CD runner) with proper warehouse credentials runs the extraction daily via cron, writes results to DynamoDB, and the Lambda serves from the cache.

```
Trusted Environment (cron) → Athena → Transform → DynamoDB
Lambda (API) → DynamoDB → Response
```

### The Lesson

When you can't fix a trust barrier, work around it. The cache pattern is actually *better* for end users — sub-second lookups instead of 30-60 second warehouse queries. The federation barrier forced a better architecture.

---

## 2. 100M+ Row Dataset Timeouts

### The Problem

The four-table JOIN across all records produced a dataset of 150M+ rows. Even Athena's distributed query engine couldn't complete the JOIN within the 900-second Lambda timeout.

### What We Tried

1. **Increased timeout** — 900s is Lambda's maximum. Still not enough.
2. **Query optimization** — Added partition pruning, predicate pushdown. Helped but not enough for the full dataset.
3. **Materialized views** — The warehouse didn't support them for federated tables.

### The Solution

**Owner-based filtering + per-table query fallback.**

Instead of joining all records at once:

1. Filter by ownership hierarchy (WHERE clause pushdown to the warehouse)
2. If the filtered JOIN still times out, fall back to per-table queries joined in Python

```python
# Strategy 1: Try the fast JOIN
try:
    rows = engine.execute_join(owner_filter=owner)
except QueryTimeoutError:
    # Strategy 2: Fall back to per-table
    rows = engine.execute_per_table(owner_filter=owner)
```

For the cache seeding script, we also added **adaptive chunking**:

```python
if total_records > 800:
    chunk_size = 30   # Very large org → tiny chunks
elif total_records > 500:
    chunk_size = 50
elif total_records > 200:
    chunk_size = 100
else:
    chunk_size = 0    # No chunking needed
```

### The Lesson

Design for the dataset you have today *and* the one you'll have tomorrow. The dual-strategy approach (fast path + safe fallback) handles both small and massive datasets without configuration changes.

---

## 3. Silent Boolean Corruption

### The Problem

After deploying the cache with ~1,400 records, the LLM reviewer started producing incorrect findings. It was flagging every application as "not processing personal data" — even ones that clearly did.

### Root Cause

The boolean parser only recognized `"true"` as truthy:

```python
def _to_bool(value):
    return str(value).lower() == "true"
```

But the data warehouse encoded booleans as `"1"` / `"0"`. Every boolean field — including `processes_personal_data` and `stores_personal_data` — was silently set to `False`.

### The Fix

```python
def _to_bool(value):
    return str(value).lower() in ("true", "1", "yes")
```

Plus a property-based test to prevent regression:

```python
@given(st.sampled_from(["true", "True", "TRUE", "1", "yes", "Yes"]))
def test_truthy_values(value):
    assert _to_bool(value) is True

@given(st.sampled_from(["false", "False", "FALSE", "0", "no", "No", "", "null"]))
def test_falsy_values(value):
    assert _to_bool(value) is False
```

### The Lesson

Boolean encoding is never simple. Different systems use different conventions. Always handle all common encodings, and use property-based tests to generate edge cases. This bug was invisible in unit tests because all test fixtures used `"true"` / `"false"`.

---

## 4. DynamoDB 400KB Item Size Limit

### The Problem

Most compliance records fit comfortably in DynamoDB's 400KB item limit. But some applications with hundreds of data stores and thousands of fields produced items of 500KB-2MB. These records were silently skipped during cache loading.

### Options Considered

| Approach | Verdict |
|---|---|
| gzip compression | Reduces size ~60%, but still hits limit for largest items |
| Split across multiple items | Complex reassembly, pagination logic, partial read risk |
| Store all data in S3 | Loses DynamoDB's sub-millisecond lookups |
| **S3 overflow** | Best of both worlds |

### The Solution

Items under 400KB go directly to DynamoDB. Items over 400KB are written to S3, and DynamoDB stores a lightweight pointer:

```python
def _write_items(self, items):
    written, overflowed = 0, 0
    for item in items:
        item_size = len(json.dumps(item).encode("utf-8"))
        if item_size > 400_000:
            # Write to S3
            s3_key = f"cache/{item['record_id']}.json"
            s3.put_object(Bucket=OVERFLOW_BUCKET, Key=s3_key, Body=json.dumps(item['data']))
            # Store pointer in DynamoDB
            dynamo_item = {
                "record_id": item["record_id"],
                "s3_key": s3_key,
                "s3_overflow": True,
                "last_updated": item["last_updated"],
            }
            table.put_item(Item=dynamo_item)
            overflowed += 1
        else:
            table.put_item(Item=item)
            written += 1
    return written, overflowed
```

The Query Lambda detects overflow items and fetches from S3 transparently:

```python
def get_record(record_id):
    item = table.get_item(Key={"record_id": record_id}).get("Item")
    if not item:
        return None
    if item.get("s3_overflow"):
        data = s3.get_object(Bucket=OVERFLOW_BUCKET, Key=item["s3_key"])
        return json.loads(data["Body"].read())
    return item["data"]
```

### The Lesson

Don't fight platform limits — work with them. S3 overflow adds minimal complexity (one `if` check on read, one on write) but eliminates the size limit entirely. The API consumer never knows the difference.

---

## 5. Credential Expiry in Automated Pipelines

### The Problem

The cache seeding cron job runs daily on a developer machine. AWS credentials come from a credential helper that issues short-lived tokens. But the credential helper itself requires an active authentication session that expires every ~10 days.

If the session expires, the cron job silently fails — no cache refresh, stale data served to users.

### The Solution

Multi-layer health checking:

1. **Pre-flight validation**: The cron wrapper validates the auth session before starting:
   ```bash
   if ! credential_helper print --account $ACCOUNT 2>/dev/null; then
       echo "Auth session expired. Run 'auth refresh' to fix."
       publish_sns_notification "FAILURE: Auth session expired"
       exit 2
   fi
   ```

2. **Health check marker**: On success, update a timestamp file:
   ```bash
   date -u +"%Y-%m-%dT%H:%M:%SZ" > logs/last_success
   ```

3. **CloudWatch metrics**: Publish `CacheRefreshFailure` metric on any failure. Alarm triggers when metric ≥ 1.

4. **SNS notifications**: Daily email with success/failure status, duration, and cache statistics.

### The Lesson

Automated pipelines on developer machines need aggressive health monitoring. The pipeline *will* break silently. Build in pre-flight checks, health markers, metrics, and notifications so you know within hours, not weeks.

---

## 6. Partial Search Performance

### The Problem

Exact lookups use DynamoDB `GetItem` — O(1), sub-millisecond. But partial/substring search requires scanning the entire table because DynamoDB doesn't support `CONTAINS` on partition keys.

### The Solution

Two-phase scan:

1. **Phase 1**: Lightweight scan with `ProjectionExpression="record_id"` — only fetches the key, not the data
2. **Phase 2**: `GetItem` for each matching key — fetches full data only for matches

```python
# Phase 1: Scan for matching keys only
response = table.scan(ProjectionExpression="record_id")
matches = [item["record_id"] for item in response["Items"]
           if search_term.lower() in item["record_id"].lower()]

# Phase 2: Fetch full data for matches
results = []
for record_id in matches:
    item = table.get_item(Key={"record_id": record_id})
    results.append(item["Item"])
```

This is acceptable for ~1,500 items (the scan returns in <500ms). For tens of thousands of items, you'd need a GSI or ElasticSearch.

### The Lesson

Know your scale. A full table scan is fine for 1,500 items but won't work for 100,000. Design for your current scale with a clear upgrade path (GSI, search index) when you need it.

---

## 7. LLM Output Inconsistency

### The Problem

The LLM review engine produces structured markdown reports. But LLMs don't always follow formatting instructions precisely — headings might be `## Gate 1:` or `### Gate 1 -` or `## GATE 1:`. The findings parser needs to handle all variations.

### The Solution

Flexible regex patterns with case-insensitive matching:

```python
_GATE_HEADING_RE = re.compile(
    r"^#{2,3}\s+[Gg][Aa][Tt][Ee]\s+(\d)\s*[:\-—]?\s*(.*)",
    re.MULTILINE
)
```

Plus fallback extraction for action items that might appear as numbered lists or checklists:

```python
# Match "1. Finding text [HIGH]" or "- [ ] Finding text [MEDIUM]"
_ACTION_ITEM_RE = re.compile(r"^\s*\d+[\.\)]\s*(.*?\[(?:HIGH|MEDIUM|LOW)\].*)", re.MULTILINE)
_CHECKLIST_RE = re.compile(r"^\s*(?:\d+[\.\)]|-)\s*\[([xX ])\]\s*(.*)", re.MULTILINE)
```

### The Lesson

When parsing LLM output, be maximally permissive in what you accept. The LLM will vary its formatting across runs. Your parser should handle all reasonable variations without breaking.
