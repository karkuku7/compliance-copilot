# Compliance Copilot

An open-source toolkit for building LLM-powered compliance review systems. Extracts data from multi-table data warehouses, caches it in a serverless API, and uses AI to perform structured compliance reviews — all exposed via the Model Context Protocol (MCP) for integration with AI agents.

## What This Project Demonstrates

This is a reference architecture for anyone building:

- **Data extraction pipelines** that join multiple warehouse tables with retry logic and deduplication
- **Serverless cache APIs** with DynamoDB + S3 overflow for large items
- **LLM-powered review engines** with prompt assembly, chunking, and structured report generation
- **MCP servers** that expose domain tools to AI agents (Claude, Copilot, etc.)
- **Infrastructure-as-code** with AWS CDK
- **Property-based testing** with Hypothesis

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Compliance Copilot                              │
│                                                                      │
│  ┌──────────────┐    ┌──────────────┐    ┌────────────────────────┐ │
│  │  API Gateway  │───▶│ Query Lambda │───▶│  DynamoDB Cache Table  │ │
│  │  GET /lookup  │    │  (256MB/90s) │    │  (PK: record_id)      │ │
│  └──────────────┘    └──────────────┘    └────────────────────────┘ │
│                                                    ▲                 │
│  ┌──────────────┐    ┌──────────────┐              │                 │
│  │  EventBridge │───▶│ Cache Loader │──────────────┘                 │
│  │  Daily       │    │  Lambda      │                                │
│  └──────────────┘    │  (512MB/900s)│    ┌────────────────────────┐ │
│                      └──────────────┘    │  S3 Overflow Bucket    │ │
│                                          │  (items > 400KB)       │ │
│  ┌──────────────────────────────────┐    └────────────────────────┘ │
│  │  Data Warehouse (Athena/SQL)     │                                │
│  │  Table A ──┐                     │                                │
│  │  Table B ──┼── Hierarchical JOIN │                                │
│  │  Table C ──┤   with dedup        │                                │
│  │  Table D ──┘                     │                                │
│  └──────────────────────────────────┘                                │
│                                                                      │
│  ┌──────────────────────────────────┐                                │
│  │  LLM Review Engine               │                                │
│  │  Prompt Assembly → LLM → Report  │                                │
│  └──────────────────────────────────┘                                │
│                                                                      │
│  ┌──────────────────────────────────┐                                │
│  │  MCP Server (stdio + SSE)        │                                │
│  │  Tools: lookup, review, findings │                                │
│  └──────────────────────────────────┘                                │
└─────────────────────────────────────────────────────────────────────┘
```

## Quick Start

```bash
# Clone and install
git clone https://github.com/YOUR_USERNAME/compliance-copilot.git
cd compliance-copilot
python3.11 -m pip install -e ".[dev]"

# Run the data extractor CLI
compliance-extract query "example-record"

# Run a compliance review
compliance-review --record "example-record" --verbose

# Start the MCP server
compliance-mcp
```

## Project Structure

```
compliance_extractor/      # Core data extraction library
  connection.py            # Data warehouse connection manager
  join_engine.py           # Multi-table hierarchical join engine
  transform.py             # Struct parsing + hierarchical transformation
  retry.py                 # Exponential backoff retry logic
  validator.py             # Dataset accessibility validation
  errors.py                # Custom exception hierarchy
  models.py                # Dataclass models
  constants.py             # Centralized configuration

compliance_reviewer/       # LLM-powered review engine
  cli.py                   # CLI entry point
  data_retriever.py        # Fetch data from cache API
  prompt_assembler.py      # Prompt construction + chunking
  llm_invoker.py           # LLM subprocess invocation
  report_generator.py      # Structured report output
  prior_review.py          # Delta analysis support
  errors.py                # Review-specific exceptions
  models.py                # Review dataclasses

compliance_mcp/            # MCP server
  server.py                # FastMCP server setup
  config.py                # Environment-based configuration
  api_client.py            # Shared HTTP client with source tracking
  tools/                   # MCP tool handlers
    lookup.py              # Record lookup tool
    review.py              # Compliance review tool
    findings.py            # Findings parser + filter
    stats.py               # Cache/usage statistics

lambdas/                   # AWS Lambda handlers
  cache_loader/            # Daily data warehouse → DynamoDB + S3
  query/                   # API Gateway request handler

scripts/                   # Operational scripts
  seed_cache.py            # Manual cache population
  run_seed_cache.sh        # Cron wrapper with health checks

infrastructure/            # AWS CDK stack
  app.py                   # CDK app entry point
  stack.py                 # Full infrastructure definition

tests/                     # Property-based + unit tests
  test_transform_properties.py
  test_transform_unit.py
  test_cache_loader.py
  test_query_handler.py
  test_retry.py

docs/
  ARCHITECTURE.md          # Deep dive on design decisions
  ROADBLOCKS.md            # Technical challenges and solutions
```

## Components

### 1. Data Extraction Library (`compliance_extractor/`)

Connects to a SQL data warehouse and joins multiple tables into a single hierarchical structure.

Key patterns:
- **Dual query strategy**: Fast single SQL JOIN with fallback to per-table queries joined in Python
- **ROW_NUMBER deduplication**: Window functions to pick latest snapshot per record
- **Struct parsing**: Recursive parser for nested key-value warehouse types
- **Retry with exponential backoff**: 1s → 2s → 4s for transient failures
- **Custom exception hierarchy**: Every error carries `recoverable` and `suggested_action`

### 2. Serverless Cache API (`lambdas/`)

Pre-joins and caches warehouse data in DynamoDB for sub-second lookups.

Key patterns:
- **S3 overflow**: Items exceeding DynamoDB's 400KB limit stored in S3 with a lightweight pointer
- **Transparent fetch**: API consumers don't know whether data comes from DynamoDB or S3
- **Usage tracking**: Atomic counters track per-record access patterns
- **Adaptive chunking**: Auto-splits large datasets to prevent OOM during cache loading

### 3. LLM Review Engine (`compliance_reviewer/`)

Assembles structured prompts from cached data and pipes them to an LLM for compliance analysis.

Key patterns:
- **External prompt files**: Customizable review criteria without code changes
- **Prior review delta**: Compare current state against previous review
- **Two output modes**: Full structured review or actionable checklist
- **No cloud credentials needed**: End users only need the CLI tool + LLM access

### 4. MCP Server (`compliance_mcp/`)

Exposes all capabilities to AI agents via the Model Context Protocol.

Key patterns:
- **Source tracking headers**: Distinguish MCP vs CLI usage in metrics
- **Self-contained tools**: Each tool is a standalone function with JSON I/O
- **Dual transport**: stdio for local agents, SSE for remote clients

## Key Design Decisions

See [ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full story. Highlights:

| Decision | Why |
|---|---|
| Cache in DynamoDB, not query live | Sub-second lookups vs 30-60s warehouse queries |
| S3 overflow, not compression | Future-proof, no size limit, transparent to consumers |
| Per-table query fallback | Single JOIN times out on 100M+ row datasets |
| LLM via subprocess, not SDK | Simpler, no model invocation code to maintain |
| External prompt files | Teams customize review criteria without PRs |
| Standalone MCP server | Full release control, no external approval needed |
| Cloud Desktop cache seeding | Workaround for data warehouse federation trust barriers |

## Testing

```bash
# Run all tests
python -m pytest

# Run property-based tests only
python -m pytest tests/test_transform_properties.py -v

# Run with coverage
python -m pytest --cov=compliance_extractor --cov=compliance_reviewer
```

The test suite uses:
- **Hypothesis** for property-based testing (transformation invariants, struct parsing roundtrips)
- **moto** for AWS service mocking (DynamoDB, S3)
- **pytest** fixtures and monkeypatch for environment isolation

## Infrastructure

Deploy with AWS CDK:

```bash
cd infrastructure
pip install -r requirements.txt
cdk synth
cdk deploy
```

Resources created:
- DynamoDB table (on-demand, PITR enabled)
- S3 overflow bucket (SSE-S3)
- Cache Loader Lambda (512MB, 900s timeout)
- Query Lambda (256MB, 90s timeout)
- API Gateway HTTP API with throttling
- EventBridge daily schedule
- CloudWatch alarm on cache refresh failures

## Contributing

Contributions welcome! Please read [CONTRIBUTING.md](CONTRIBUTING.md) before submitting PRs.

## License

MIT License — see [LICENSE](LICENSE) for details.
