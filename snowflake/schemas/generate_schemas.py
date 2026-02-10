#!/usr/bin/env python3
"""
Generate schema documentation for the Snowflake DBT_DEMO database.
Extracts all tables and views from the DEV schema and produces
a Markdown documentation file for dbt models (staging, intermediate, marts).

Usage:
    python generate_schemas.py

Environment variables (required):
    SNOWFLAKE_ACCOUNT   - Snowflake account identifier
    SNOWFLAKE_USER      - Snowflake username
    SNOWFLAKE_PASSWORD  - Snowflake password

Hard-coded connection parameters (from dbt profiles.yml):
    Warehouse: COMPUTE_WH
    Database:  DBT_DEMO
    Role:      ACCOUNTADMIN
    Schema:    DEV
"""

import os
import sys

try:
    import snowflake.connector
except ImportError:
    print("ERROR: snowflake-connector-python is required.")
    print("Install with: pip install snowflake-connector-python")
    sys.exit(1)


def get_connection():
    """
    Establish Snowflake connection using environment variables and hard-coded parameters.
    
    Required environment variables:
    - SNOWFLAKE_ACCOUNT
    - SNOWFLAKE_USER
    - SNOWFLAKE_PASSWORD
    
    Raises:
        KeyError: If required environment variables are not set.
    """
    try:
        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse="COMPUTE_WH",
            database="DBT_DEMO",
            role="ACCOUNTADMIN",
            schema="DEV",
        )
        return conn
    except KeyError as e:
        raise KeyError(f"Missing required environment variable: {e}")


def get_relations(cur):
    """Get all tables and views in the DEV schema (dbt models)."""
    cur.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_catalog = CURRENT_DATABASE()
          AND table_schema = CURRENT_SCHEMA()
          AND table_schema NOT IN ('INFORMATION_SCHEMA')
        ORDER BY table_type DESC, table_name;
    """)
    return cur.fetchall()


def get_columns(cur, schema, table_name):
    cur.execute("""
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_catalog = CURRENT_DATABASE()
          AND table_schema = %s
          AND table_name = %s
        ORDER BY ordinal_position;
    """, (schema, table_name))
    return cur.fetchall()


def get_primary_keys(cur, schema, table_name):
    cur.execute(f"""
        SHOW PRIMARY KEYS IN "{schema}"."{table_name}";
    """)
    rows = cur.fetchall()
    return set(row[4] for row in rows)  # column_name is index 4


def get_foreign_keys(cur, schema, table_name):
    cur.execute(f"""
        SHOW IMPORTED KEYS IN "{schema}"."{table_name}";
    """)
    rows = cur.fetchall()
    # fk_column, pk_schema, pk_table, pk_column
    return [(row[7], row[1], row[2], row[3]) for row in rows]


def get_row_count(cur, schema, table_name):
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}";')
        return cur.fetchone()[0]
    except Exception:
        return "N/A"


def get_clustering_info(cur, schema, table_name):
    try:
        cur.execute(f"""
            SELECT system$clustering_information('"{schema}"."{table_name}"');
        """)
        return cur.fetchone()[0]
    except Exception:
        return None


def get_table_comment(cur, schema, table_name):
    cur.execute("""
        SELECT comment
        FROM information_schema.tables
        WHERE table_catalog = CURRENT_DATABASE()
          AND table_schema = %s AND table_name = %s;
    """, (schema, table_name))
    row = cur.fetchone()
    return row[0] if row and row[0] else None


def format_data_type(col):
    _, data_type, char_max_len, num_precision, num_scale, _, _ = col
    dt = data_type.upper()
    if dt in ("TEXT", "VARCHAR") and char_max_len:
        return f"VARCHAR({char_max_len})"
    elif dt == "NUMBER" and num_precision is not None and num_scale is not None:
        return f"NUMBER({num_precision},{num_scale})"
    return dt


def generate_markdown(cur):
    """
    Generate comprehensive Markdown documentation for all dbt models in the DEV schema.
    
    Includes:
    - Staging models (11 views)
    - Intermediate models (8 views)
    - Marts models (10 views)
    
    For each model:
    - Column details (name, type, nullability, defaults)
    - Primary keys (where applicable)
    - Row counts
    - View definitions (for non-seed tables)
    """
    relations = get_relations(cur)
    lines = []
    lines.append("# Bain Capital Analytics — Snowflake Schema Documentation")
    lines.append("")
    lines.append("> Auto-generated from the live Snowflake `DBT_DEMO` database, `DEV` schema.")
    lines.append("> Documents all dbt-generated models: staging, intermediate, and marts layers.")
    lines.append("")
    lines.append(f"**Database:** `DBT_DEMO` | **Schema:** `DEV`  ")

    schemas = sorted(set(r[0] for r in relations))
    tables = [r for r in relations if r[2] == "BASE TABLE"]
    views = [r for r in relations if r[2] == "VIEW"]
    total = len(tables) + len(views)
    lines.append(f"**Total Objects:** {total} ({len(views)} views, {len(tables)} tables)  ")
    lines.append("")

    # Table of contents (by view type/layer)
    lines.append("## Table of Contents")
    lines.append("")
    
    # Categorize by naming convention (stg_, int_, fact_, report_)
    stg_models = sorted([r for r in relations if r[1].startswith("stg_")], key=lambda x: x[1])
    int_models = sorted([r for r in relations if r[1].startswith("int_")], key=lambda x: x[1])
    fact_models = sorted([r for r in relations if r[1].startswith("fact_")], key=lambda x: x[1])
    report_models = sorted([r for r in relations if r[1].startswith("report_")], key=lambda x: x[1])
    raw_models = sorted([r for r in relations if r[1].startswith("raw_")], key=lambda x: x[1])
    other_models = sorted([r for r in relations 
                           if not any(r[1].startswith(prefix) for prefix in ["stg_", "int_", "fact_", "report_", "raw_"])],
                          key=lambda x: x[1])
    
    if stg_models:
        lines.append("### Staging Models (dbt source layer)")
        lines.append("")
        for s, name, rtype in stg_models:
            anchor = name.replace("_", "-")
            lines.append(f"- [{name}](#{anchor})")
        lines.append("")
    
    if int_models:
        lines.append("### Intermediate Models (dbt intermediate layer)")
        lines.append("")
        for s, name, rtype in int_models:
            anchor = name.replace("_", "-")
            lines.append(f"- [{name}](#{anchor})")
        lines.append("")
    
    if fact_models:
        lines.append("### Fact Models (dbt marts layer)")
        lines.append("")
        for s, name, rtype in fact_models:
            anchor = name.replace("_", "-")
            lines.append(f"- [{name}](#{anchor})")
        lines.append("")
    
    if report_models:
        lines.append("### Report Models (dbt marts layer)")
        lines.append("")
        for s, name, rtype in report_models:
            anchor = name.replace("_", "-")
            lines.append(f"- [{name}](#{anchor})")
        lines.append("")
    
    if raw_models:
        lines.append("### Raw Tables (seeded data)")
        lines.append("")
        for s, name, rtype in raw_models:
            anchor = name.replace("_", "-")
            lines.append(f"- [{name}](#{anchor})")
        lines.append("")
    
    if other_models:
        lines.append("### Other Objects")
        lines.append("")
        for s, name, rtype in other_models:
            anchor = name.replace("_", "-")
            lines.append(f"- [{name}](#{anchor})")
        lines.append("")

    lines.append("---")
    lines.append("")

    for schema, table_name, table_type in relations:
        row_count = get_row_count(cur, schema, table_name)
        columns = get_columns(cur, schema, table_name)
        badge = "TABLE" if table_type == "BASE TABLE" else "VIEW"
        comment = get_table_comment(cur, schema, table_name)

        anchor = table_name.replace("_", "-")
        lines.append(f"## `{table_name}`")
        lines.append("")
        lines.append(f"**Type:** `{badge}` | **Rows:** {row_count}  ")
        if comment:
            lines.append(f"**Comment:** {comment}  ")
        lines.append("")

        # Primary keys
        pk_cols = set()
        if table_type == "BASE TABLE":
            try:
                pk_cols = get_primary_keys(cur, schema, table_name)
            except Exception:
                pass

        # Columns
        lines.append("| # | Column | Type | Nullable | Default | PK |")
        lines.append("|---|--------|------|----------|---------|-----|")
        for i, col in enumerate(columns, 1):
            col_name = col[0]
            dtype = format_data_type(col)
            nullable = "YES" if col[5] == "YES" else "NO"
            default = str(col[6]) if col[6] else ""
            if len(default) > 40:
                default = default[:37] + "..."
            pk = "PK" if col_name in pk_cols else ""
            lines.append(f"| {i} | `{col_name}` | `{dtype}` | {nullable} | {default} | {pk} |")
        lines.append("")

        # Foreign keys for tables
        if table_type == "BASE TABLE":
            try:
                fks = get_foreign_keys(cur, schema, table_name)
                if fks:
                    lines.append("**Foreign Keys:**")
                    lines.append("")
                    for fk_col, ref_schema, ref_table, ref_col in fks:
                        lines.append(f"- `{fk_col}` -> `{ref_schema}.{ref_table}({ref_col})`")
                    lines.append("")
            except Exception:
                pass

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def main():
    """
    Main entry point: validate environment variables, connect to Snowflake,
    generate schema documentation, and write to markdown file.
    """
    required_vars = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing = [v for v in required_vars if v not in os.environ]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("Please set these environment variables before running the script:")
        for var in missing:
            print(f"  export {var}=<value>")
        sys.exit(1)

    try:
        print("Connecting to Snowflake...")
        conn = get_connection()
        print("✓ Connected successfully")
        
        cur = conn.cursor()
        try:
            print("Querying schema information...")
            md = generate_markdown(cur)
            
            output_path = os.path.join(os.path.dirname(__file__), "all_schemas_snowflake.md")
            with open(output_path, "w") as f:
                f.write(md)
            
            print(f"✓ Schema documentation written to: {output_path}")
            
            # Print summary
            relations = get_relations(cur)
            tables = [r for r in relations if r[2] == "BASE TABLE"]
            views = [r for r in relations if r[2] == "VIEW"]
            print(f"✓ Documentation complete: {len(tables)} tables, {len(views)} views documented")
            
        finally:
            cur.close()
    except Exception as e:
        print(f"ERROR: {type(e).__name__}: {e}")
        sys.exit(1)
    finally:
        try:
            conn.close()
        except:
            pass


if __name__ == "__main__":
    main()
