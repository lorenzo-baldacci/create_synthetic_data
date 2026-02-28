#!/usr/bin/env python3
"""CLI tool to generate synthetic data and load it into a Unity Catalog Delta table."""

import argparse
import sys

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

from src.data_generator import generate_dataframe
from src.schema_parser import parse_schema
from src.validator import ensure_schema, validate_catalog


def build_ddl(col_tuples):
    """Build a SQL column definition string from a list of (name, sql_type) tuples."""
    parts = [f"`{name}` {sql_type}" for name, sql_type in col_tuples]
    return ", ".join(parts)


def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic data and load it into a Unity Catalog Delta table."
    )
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument("--schema", required=True, help="Unity Catalog schema name")
    parser.add_argument("--table", required=True, help="Unity Catalog table name")
    parser.add_argument("--schema-file", required=True, help="Path to JSON schema file")
    parser.add_argument("--rows", type=int, default=1000, help="Number of rows to generate (default: 1000)")
    parser.add_argument("--profile", default="AZURE_FE", help="Databricks config profile (default: AZURE_FE)")
    parser.add_argument("--cluster-id", default=None, help="Databricks cluster ID (uses serverless if omitted)")
    args = parser.parse_args()

    # 1. Parse schema file
    print(f"Parsing schema file: {args.schema_file}")
    try:
        struct_type, col_tuples = parse_schema(args.schema_file)
    except (ValueError, KeyError, FileNotFoundError) as e:
        print(f"Error reading schema file: {e}", file=sys.stderr)
        sys.exit(1)
    print(f"Parsed {len(col_tuples)} columns: {[c[0] for c in col_tuples]}")

    # 2. Validate UC access
    ws = WorkspaceClient(profile=args.profile)
    validate_catalog(ws, args.catalog)
    ensure_schema(ws, args.catalog, args.schema)

    # 3. Init Spark via Databricks Connect
    builder = DatabricksSession.builder.profile(args.profile)
    if args.cluster_id:
        print(f"Connecting to cluster {args.cluster_id} via Databricks Connect...")
        builder = builder.clusterId(args.cluster_id)
    else:
        print("Connecting via Databricks Connect (serverless)...")
        builder = builder.serverless(True)
    spark = builder.getOrCreate()

    full_table = f"`{args.catalog}`.`{args.schema}`.`{args.table}`"

    # 4. Recreate table
    print(f"Dropping table if exists: {full_table}")
    spark.sql(f"DROP TABLE IF EXISTS {full_table}")

    ddl_str = build_ddl(col_tuples)
    print(f"Creating table: {full_table}")
    spark.sql(f"CREATE TABLE {full_table} ({ddl_str})")

    # 5. Generate data
    print(f"Generating {args.rows} rows of synthetic data...")
    pdf = generate_dataframe(col_tuples, args.rows)

    # 6. Fix timezone-naive datetimes — Spark TimestampType requires UTC-aware timestamps
    import pandas as pd
    for col_name, col_type in col_tuples:
        if col_type.upper() == "TIMESTAMP":
            pdf[col_name] = pd.to_datetime(pdf[col_name]).dt.tz_localize("UTC")

    # 7. Write to table
    print("Writing data to Delta table...")
    spark.createDataFrame(pdf, schema=struct_type).write.mode("append").saveAsTable(
        f"{args.catalog}.{args.schema}.{args.table}"
    )

    print(
        f"\nDone! {args.rows} rows written to {args.catalog}.{args.schema}.{args.table}"
    )


if __name__ == "__main__":
    main()
