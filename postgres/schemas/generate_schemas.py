#!/usr/bin/env python3
"""
Generate schema documentation for the PostgreSQL bain_analytics database.
Extracts all tables and views across all application schemas and produces
a Markdown documentation file.

Usage:
    python generate_schemas.py

Environment variables (optional):
    PG_HOST     - default: localhost
    PG_PORT     - default: 5433
    PG_USER     - default: postgres
    PG_PASSWORD - default: postgres
    PG_DBNAME   - default: bain_analytics
"""

import os
import sys

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 is required. Install with: pip install psycopg2-binary")
    sys.exit(1)


def get_connection():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST", "localhost"),
        port=int(os.environ.get("PG_PORT", "5433")),
        user=os.environ.get("PG_USER", "postgres"),
        password=os.environ.get("PG_PASSWORD", "postgres"),
        dbname=os.environ.get("PG_DBNAME", "bain_analytics"),
    )


def get_relations(cur):
    """Get all tables and views in application schemas (exclude pg_ and information_schema)."""
    cur.execute("""
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_type DESC, table_name;
    """)
    return cur.fetchall()


def get_columns(cur, schema, table_name):
    cur.execute("""
        SELECT
            c.column_name,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default
        FROM information_schema.columns c
        WHERE c.table_schema = %s
          AND c.table_name = %s
        ORDER BY c.ordinal_position;
    """, (schema, table_name))
    return cur.fetchall()


def get_constraints(cur, schema, table_name):
    cur.execute("""
        SELECT
            tc.constraint_name,
            tc.constraint_type,
            kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = %s
          AND tc.table_name = %s
        ORDER BY tc.constraint_type, tc.constraint_name, kcu.ordinal_position;
    """, (schema, table_name))
    return cur.fetchall()


def get_indexes(cur, schema, table_name):
    cur.execute("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE schemaname = %s AND tablename = %s
        ORDER BY indexname;
    """, (schema, table_name))
    return cur.fetchall()


def get_check_constraints(cur, schema, table_name):
    cur.execute("""
        SELECT conname, pg_get_constraintdef(c.oid)
        FROM pg_constraint c
        JOIN pg_class t ON c.conrelid = t.oid
        JOIN pg_namespace n ON t.relnamespace = n.oid
        WHERE n.nspname = %s AND t.relname = %s AND c.contype = 'c'
        ORDER BY conname;
    """, (schema, table_name))
    return cur.fetchall()


def get_foreign_keys(cur, schema, table_name):
    cur.execute("""
        SELECT
            tc.constraint_name,
            kcu.column_name,
            ccu.table_schema AS foreign_schema,
            ccu.table_name AS foreign_table,
            ccu.column_name AS foreign_column
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
        WHERE tc.constraint_type = 'FOREIGN KEY'
          AND tc.table_schema = %s AND tc.table_name = %s
        ORDER BY tc.constraint_name;
    """, (schema, table_name))
    return cur.fetchall()


def get_row_count(cur, schema, table_name):
    try:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}";')
        return cur.fetchone()[0]
    except Exception:
        return "N/A"


def get_view_definition(cur, schema, table_name):
    cur.execute("""
        SELECT view_definition
        FROM information_schema.views
        WHERE table_schema = %s AND table_name = %s;
    """, (schema, table_name))
    row = cur.fetchone()
    return row[0].strip() if row else None


def format_data_type(col):
    _, data_type, char_max_len, num_precision, num_scale, _, _ = col
    if data_type == "character varying" and char_max_len:
        return f"VARCHAR({char_max_len})"
    elif data_type == "numeric" and num_precision and num_scale is not None:
        return f"NUMERIC({num_precision},{num_scale})"
    elif data_type == "integer":
        return "INTEGER"
    elif data_type == "bigint":
        return "BIGINT"
    elif data_type == "boolean":
        return "BOOLEAN"
    elif data_type == "date":
        return "DATE"
    elif data_type == "text":
        return "TEXT"
    elif data_type == "double precision":
        return "DOUBLE PRECISION"
    else:
        return data_type.upper()


def generate_markdown(cur):
    relations = get_relations(cur)
    lines = []
    lines.append("# Bain Capital Analytics — PostgreSQL Schema Documentation")
    lines.append("")
    lines.append("> Auto-generated from the live `bain_analytics` PostgreSQL database.")
    lines.append("")
    lines.append(f"**Database:** `bain_analytics`  ")

    # Count by type
    schemas = sorted(set(r[0] for r in relations))
    tables = [r for r in relations if r[2] == "BASE TABLE"]
    views = [r for r in relations if r[2] == "VIEW"]
    lines.append(f"**Schemas:** {', '.join(f'`{s}`' for s in schemas)}  ")
    lines.append(f"**Tables:** {len(tables)} | **Views:** {len(views)}  ")
    lines.append("")

    # Table of contents grouped by schema
    lines.append("## Table of Contents")
    lines.append("")
    for schema in schemas:
        lines.append(f"### Schema: `{schema}`")
        lines.append("")
        schema_rels = [r for r in relations if r[0] == schema]
        for s, name, rtype in schema_rels:
            badge = "TABLE" if rtype == "BASE TABLE" else "VIEW"
            anchor = f"{schema}-{name}".replace("_", "-")
            lines.append(f"- [{name}](#{anchor}) `{badge}`")
        lines.append("")

    lines.append("---")
    lines.append("")

    for schema, table_name, table_type in relations:
        row_count = get_row_count(cur, schema, table_name)
        columns = get_columns(cur, schema, table_name)
        badge = "TABLE" if table_type == "BASE TABLE" else "VIEW"

        lines.append(f"## `{schema}`.`{table_name}`")
        lines.append("")
        lines.append(f"**Type:** `{badge}` | **Rows:** {row_count}  ")
        lines.append("")

        # Columns
        lines.append("| # | Column | Type | Nullable | Default | PK |")
        lines.append("|---|--------|------|----------|---------|-----|")

        pk_cols = set()
        if table_type == "BASE TABLE":
            constraints = get_constraints(cur, schema, table_name)
            for cname, ctype, col in constraints:
                if ctype == "PRIMARY KEY":
                    pk_cols.add(col)

        for i, col in enumerate(columns, 1):
            col_name = col[0]
            dtype = format_data_type(col)
            nullable = "YES" if col[5] == "YES" else "NO"
            default = str(col[6]) if col[6] else ""
            # Truncate long defaults
            if len(default) > 40:
                default = default[:37] + "..."
            pk = "PK" if col_name in pk_cols else ""
            lines.append(f"| {i} | `{col_name}` | `{dtype}` | {nullable} | {default} | {pk} |")
        lines.append("")

        # Only for base tables: FK, CHECK, indexes
        if table_type == "BASE TABLE":
            fks = get_foreign_keys(cur, schema, table_name)
            if fks:
                lines.append("**Foreign Keys:**")
                lines.append("")
                for fk_name, fk_col, ref_schema, ref_table, ref_col in fks:
                    lines.append(f"- `{fk_col}` -> `{ref_schema}.{ref_table}({ref_col})`")
                lines.append("")

            checks = get_check_constraints(cur, schema, table_name)
            if checks:
                lines.append("**Check Constraints:**")
                lines.append("")
                for chk_name, chk_def in checks:
                    lines.append(f"- `{chk_name}`: `{chk_def}`")
                lines.append("")

            indexes = get_indexes(cur, schema, table_name)
            if indexes:
                lines.append("**Indexes:**")
                lines.append("")
                for idx_name, idx_def in indexes:
                    lines.append(f"- `{idx_name}`")
                lines.append("")

        # For views: show upstream dependencies (simplified)
        if table_type == "VIEW":
            view_def = get_view_definition(cur, schema, table_name)
            if view_def:
                lines.append("<details>")
                lines.append("<summary>View Definition (click to expand)</summary>")
                lines.append("")
                lines.append("```sql")
                lines.append(view_def)
                lines.append("```")
                lines.append("")
                lines.append("</details>")
                lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def main():
    conn = get_connection()
    cur = conn.cursor()
    try:
        md = generate_markdown(cur)
        output_path = os.path.join(os.path.dirname(__file__), "all_schemas_postgres.md")
        with open(output_path, "w") as f:
            f.write(md)
        print(f"Schema documentation written to: {output_path}")
        # Print summary
        relations = get_relations(cur)
        tables = [r for r in relations if r[2] == "BASE TABLE"]
        views = [r for r in relations if r[2] == "VIEW"]
        print(f"  {len(tables)} tables, {len(views)} views documented")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
