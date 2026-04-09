"""CLI entry point for the compliance data extractor.

Usage:
    compliance-extract query <record_id>
    compliance-extract query <record_id> --partial
    compliance-extract extract --owners alice,bob
"""

import argparse
import json
import logging
import sys


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="compliance-extract",
        description="Compliance data extraction CLI",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Debug logging")

    subparsers = parser.add_subparsers(dest="command", required=True)

    # query subcommand
    query_parser = subparsers.add_parser("query", help="Query a specific record")
    query_parser.add_argument("record_id", help="Record ID to look up")
    query_parser.add_argument(
        "--partial", action="store_true", help="Use LIKE match for partial names"
    )

    # extract subcommand
    extract_parser = subparsers.add_parser("extract", help="Run full extraction")
    extract_parser.add_argument(
        "--owners", help="Comma-separated owner logins to filter by"
    )
    extract_parser.add_argument(
        "--per-table",
        action="store_true",
        help="Use per-table query strategy instead of JOIN",
    )
    extract_parser.add_argument(
        "--timeout", type=int, default=300, help="Query timeout in seconds"
    )

    return parser


def main() -> None:
    parser = create_parser()
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if args.command == "query":
        _handle_query(args)
    elif args.command == "extract":
        _handle_extract(args)


def _handle_query(args: argparse.Namespace) -> None:
    """Handle the query subcommand."""
    from compliance_extractor.connection import ConnectionManager
    from compliance_extractor.join_engine import JoinEngine

    mgr = ConnectionManager()
    session = mgr.connect()
    engine = JoinEngine()

    try:
        rows = engine.execute_join(
            session,
            record_ids=[args.record_id],
            timeout_seconds=300,
        )
        if not rows:
            print(f"No records found for '{args.record_id}'", file=sys.stderr)
            sys.exit(1)

        from compliance_extractor.transform import transform_rows_to_hierarchical

        hierarchical = transform_rows_to_hierarchical(rows)
        print(json.dumps(hierarchical, indent=2, default=str))
    finally:
        session.close()


def _handle_extract(args: argparse.Namespace) -> None:
    """Handle the extract subcommand."""
    from compliance_extractor.connection import ConnectionManager
    from compliance_extractor.join_engine import JoinEngine
    from compliance_extractor.transform import transform_rows_to_hierarchical

    mgr = ConnectionManager()
    session = mgr.connect()
    engine = JoinEngine()

    owners = args.owners.split(",") if args.owners else None

    try:
        if args.per_table:
            rows = engine.execute_per_table(
                session, owner_logins=owners, timeout_seconds=args.timeout
            )
        else:
            try:
                rows = engine.execute_join(
                    session, owner_logins=owners, timeout_seconds=args.timeout
                )
            except Exception:
                logging.getLogger(__name__).warning(
                    "JOIN timed out, falling back to per-table strategy"
                )
                rows = engine.execute_per_table(
                    session, owner_logins=owners, timeout_seconds=args.timeout
                )

        hierarchical = transform_rows_to_hierarchical(rows)
        print(json.dumps(hierarchical, indent=2, default=str))
        print(
            f"\nExtracted {len(hierarchical)} records from {len(rows)} rows",
            file=sys.stderr,
        )
    finally:
        session.close()


if __name__ == "__main__":
    main()
