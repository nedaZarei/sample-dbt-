# Snowflake Schema Documentation Generator

This script generates comprehensive Markdown documentation for all dbt models deployed to Snowflake.

## Overview

The `generate_schemas.py` script connects to a Snowflake instance and extracts metadata about all dbt-generated models (views and tables) in the `DEV` schema of the `DBT_DEMO` database.

### Supported Objects

The script documents:

- **Staging Models** (`stg_*`) - 10 views
  - Data layer that closely mirrors raw source tables
  - Includes: benchmarks, cashflows, counterparties, dates, fund structures, instruments, portfolios, positions, trades, valuations

- **Intermediate Models** (`int_*`) - 8 views  
  - Transformation layer with business logic
  - Includes: benchmark returns, cashflow enriched, daily positions, fund nav, irr calculations, portfolio attribution, trade enriched, valuation enriched

- **Marts Models** - 10 views
  - Final fact and report tables for consumption
  - Fact models (6): cashflow waterfall, fund performance, portfolio attribution, portfolio PnL, portfolio summary, trade activity
  - Report models (4): daily PnL, IC dashboard, LP quarterly, portfolio overview

- **Raw Tables** (`raw_*`) - 10 tables (seeded from CSV)
  - Base data loaded via `dbt seed`
  - Includes same entities as staging models

## Usage

### Prerequisites

1. **Python 3.6+** with `snowflake-connector-python` installed:
   ```bash
   pip install snowflake-connector-python
   ```

2. **Environment Variables** (required):
   - `SNOWFLAKE_ACCOUNT` - Your Snowflake account identifier (e.g., `xy12345.us-east-1`)
   - `SNOWFLAKE_USER` - Snowflake username
   - `SNOWFLAKE_PASSWORD` - Snowflake password

3. **Snowflake Configuration** (hard-coded):
   - Warehouse: `COMPUTE_WH`
   - Database: `DBT_DEMO`
   - Role: `ACCOUNTADMIN`
   - Schema: `DEV`

### Running the Script

```bash
# Set environment variables (Linux/macOS)
export SNOWFLAKE_ACCOUNT=<your_account>
export SNOWFLAKE_USER=<your_username>
export SNOWFLAKE_PASSWORD=<your_password>

# Run the script
cd snowflake/schemas
python generate_schemas.py
```

### Output

The script generates `all_schemas_snowflake.md` in the same directory with:

- **Header** - Database, schema, and object counts
- **Table of Contents** - Organized by model layer (staging, intermediate, marts, raw)
- **Model Sections** - For each object:
  - Type (VIEW or TABLE)
  - Row count
  - Columns with data types, nullability, defaults, and PK status
  - Primary keys and foreign keys (where applicable)

## Example Output Structure

```markdown
# Bain Capital Analytics — Snowflake Schema Documentation

> Auto-generated from the live Snowflake `DBT_DEMO` database, `DEV` schema.
> Documents all dbt-generated models: staging, intermediate, and marts layers.

**Database:** `DBT_DEMO` | **Schema:** `DEV`
**Total Objects:** 38 (28 views, 10 tables)

## Table of Contents

### Staging Models (dbt source layer)
- [stg_benchmarks](#stg-benchmarks)
- [stg_cashflows](#stg-cashflows)
...

### Intermediate Models (dbt intermediate layer)
- [int_benchmark_returns](#int-benchmark-returns)
...

### Fact Models (dbt marts layer)
- [fact_cashflow_waterfall](#fact-cashflow-waterfall)
...

---

## `stg_benchmarks`

**Type:** `VIEW` | **Rows:** 30

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `benchmark_id` | `VARCHAR` | NO |  | PK |
| 2 | `benchmark_name` | `VARCHAR` | YES |  |  |
...
```

## Dependencies

### Task Prerequisites

- **Task #2**: `snowflake/profiles.yml` configured with Snowflake credentials
- **Task #3, #4, #5**: All dbt models must be built via `dbt run` and `dbt seed` commands
  - Models must exist in the `DEV` schema of `DBT_DEMO` database
  - Seed files must be loaded

### External Dependencies

- `snowflake-connector-python` - Official Snowflake Python connector
- Python standard library: `os`, `sys`

## Troubleshooting

### Connection Errors

**Error**: `Missing required environment variable: 'SNOWFLAKE_ACCOUNT'`
- **Solution**: Set the `SNOWFLAKE_ACCOUNT` environment variable before running

**Error**: `403: Could not authenticate`
- **Solution**: Verify SNOWFLAKE_USER and SNOWFLAKE_PASSWORD are correct

**Error**: `Database 'DBT_DEMO' does not exist`
- **Solution**: Ensure dbt has been run with correct warehouse/database setup (see `snowflake/profiles.yml`)

### Missing Objects

**Error**: Script runs but produces minimal output
- **Solution**: Ensure `dbt seed` and `dbt run` have been executed successfully
- Check that models exist: `SHOW TABLES IN DBT_DEMO.DEV;` (in Snowflake console)

## Modifications for Different Environments

To adapt this script for a different Snowflake environment:

1. **Update connection parameters** in `get_connection()` function:
   ```python
   warehouse="COMPUTE_WH",      # Change this
   database="DBT_DEMO",          # Change this
   role="ACCOUNTADMIN",          # Change this
   schema="DEV",                 # Change this
   ```

2. **Update header documentation** in `generate_markdown()` to reflect your environment

3. Re-run the script with appropriate environment variables

## Success Criteria

✅ Script executes without errors when environment variables are set
✅ Connects successfully to Snowflake using provided credentials
✅ Generates `all_schemas_snowflake.md` in snowflake/schemas/
✅ Documentation includes all 28 dbt models + 10 seed tables:
  - 10 staging views
  - 8 intermediate views
  - 10 marts views (6 fact + 4 report)
  - 10 seed tables
✅ Each object includes:
  - Column names, data types, nullability
  - Row counts
  - Primary keys and foreign keys
✅ Markdown formatting matches the PostgreSQL version structure
✅ No missing models or incomplete documentation

## Version History

- **v1.0** - Snowflake-specific implementation for DBT_DEMO database
  - Uses `snowflake-connector-python` instead of psycopg2
  - Hard-coded connection to COMPUTE_WH, DBT_DEMO, ACCOUNTADMIN, DEV
  - Improved error handling and status messages
  - Support for dbt model categorization (staging, intermediate, marts)
