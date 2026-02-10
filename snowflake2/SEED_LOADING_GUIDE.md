# Snowflake Seed Loading Guide

## Overview
This guide documents the process for loading 10 seed CSV files into Snowflake to create foundation data tables that staging models depend on.

## Prerequisites

### 1. Snowflake Connection
Ensure the following environment variables are set:
```bash
export SNOWFLAKE_ACCOUNT="your_account_identifier"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
```

### 2. Database and Schema
- **Database**: `DBT_DEMO` (must exist)
- **Target Schema**: `RAW` (will be created automatically by dbt)
- Note: The dbt_project.yml configures seeds with `+schema: raw`, so tables will be created in `DBT_DEMO.RAW` schema

### 3. Warehouse
- **Warehouse**: `COMPUTE_WH` (must exist and be accessible)

## Seed Files Inventory

All seed files are located in `snowflake2/seeds/` directory:

| Seed File | Expected Rows | Columns | Description |
|-----------|---------------|---------|-------------|
| raw_benchmarks.csv | 30 | 7 | Benchmark performance data (S&P 500, US High Yield) |
| raw_cashflows.csv | 25 | 9 | Fund cashflows (capital calls, distributions, fees) |
| raw_counterparties.csv | 10 | 6 | Trading counterparty information |
| raw_dates.csv | 15 | 7 | Date dimension with business day flags |
| raw_fund_structures.csv | 3 | 11 | Fund structure and fee information |
| raw_instruments.csv | 20 | 8 | Financial instruments (equities, bonds, PE, etc.) |
| raw_portfolios.csv | 5 | 7 | Portfolio metadata and strategies |
| raw_positions.csv | 30 | 8 | Historical position data |
| raw_trades.csv | 30 | 13 | Trade execution records |
| raw_valuations.csv | 21 | 9 | Portfolio valuations and P&L |

**Total: 10 files, 189 rows**

## Execution Steps

### Step 1: Navigate to Project Directory
```bash
cd snowflake2/
```

### Step 2: Verify Configuration
```bash
# Check profiles.yml exists and is configured correctly
cat profiles.yml

# Expected output should show:
#   type: snowflake
#   account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
#   database: DBT_DEMO
#   schema: DEV (default schema, but seeds will use RAW)
```

### Step 3: Run dbt Seed Command
```bash
dbt seed --profiles-dir .
```

### Alternative: Seed Specific Tables
```bash
# Load only specific seed files
dbt seed --select raw_portfolios --profiles-dir .
dbt seed --select raw_instruments --profiles-dir .
```

### Step 4: Verify in Snowflake
```sql
-- Check all tables exist in RAW schema
USE DATABASE DBT_DEMO;
USE SCHEMA RAW;

SHOW TABLES IN DBT_DEMO.RAW;

-- Verify row counts
SELECT 'raw_benchmarks' as table_name, COUNT(*) as row_count FROM raw_benchmarks
UNION ALL
SELECT 'raw_cashflows', COUNT(*) FROM raw_cashflows
UNION ALL
SELECT 'raw_counterparties', COUNT(*) FROM raw_counterparties
UNION ALL
SELECT 'raw_dates', COUNT(*) FROM raw_dates
UNION ALL
SELECT 'raw_fund_structures', COUNT(*) FROM raw_fund_structures
UNION ALL
SELECT 'raw_instruments', COUNT(*) FROM raw_instruments
UNION ALL
SELECT 'raw_portfolios', COUNT(*) FROM raw_portfolios
UNION ALL
SELECT 'raw_positions', COUNT(*) FROM raw_positions
UNION ALL
SELECT 'raw_trades', COUNT(*) FROM raw_trades
UNION ALL
SELECT 'raw_valuations', COUNT(*) FROM raw_valuations;
```

## Expected Output

### Successful Seed Output
```
Running with dbt=1.x.x
Found 10 seeds, ...

Concurrency: 4 threads

Building catalog...

Seed raw_benchmarks.............. [RUN]
Seed raw_cashflows............... [RUN]
Seed raw_counterparties.......... [RUN]
Seed raw_dates................... [RUN]
...

Seed raw_benchmarks.............. [SUCCESS in 2.3s]
Seed raw_cashflows............... [SUCCESS in 1.8s]
Seed raw_counterparties.......... [SUCCESS in 1.5s]
Seed raw_dates................... [SUCCESS in 1.2s]
Seed raw_fund_structures......... [SUCCESS in 1.1s]
Seed raw_instruments............. [SUCCESS in 1.6s]
Seed raw_portfolios.............. [SUCCESS in 1.3s]
Seed raw_positions............... [SUCCESS in 1.9s]
Seed raw_trades.................. [SUCCESS in 2.1s]
Seed raw_valuations.............. [SUCCESS in 1.7s]

Finished running 10 seeds in X.XXs.

Completed successfully
```

## Expected Tables in Snowflake

After successful execution, the following tables will exist in `DBT_DEMO.RAW` schema:

1. **RAW_BENCHMARKS** - 30 rows
2. **RAW_CASHFLOWS** - 25 rows
3. **RAW_COUNTERPARTIES** - 10 rows
4. **RAW_DATES** - 15 rows
5. **RAW_FUND_STRUCTURES** - 3 rows
6. **RAW_INSTRUMENTS** - 20 rows
7. **RAW_PORTFOLIOS** - 5 rows
8. **RAW_POSITIONS** - 30 rows
9. **RAW_TRADES** - 30 rows
10. **RAW_VALUATIONS** - 21 rows

## Troubleshooting

### Error: Database 'DBT_DEMO' does not exist
```sql
-- Create database (requires ACCOUNTADMIN role)
CREATE DATABASE IF NOT EXISTS DBT_DEMO;
```

### Error: Warehouse 'COMPUTE_WH' does not exist
```sql
-- Create warehouse (requires ACCOUNTADMIN role)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH 
  WITH WAREHOUSE_SIZE = 'X-SMALL' 
  AUTO_SUSPEND = 60 
  AUTO_RESUME = TRUE;
```

### Error: Connection timeout or authentication failure
- Verify environment variables are set correctly
- Check SNOWFLAKE_ACCOUNT format (should be: `account_identifier.region.cloud_provider`)
- Ensure user has necessary privileges (SYSADMIN or ACCOUNTADMIN role)

### Error: CSV parsing errors
- Check CSV files for malformed data
- Verify column headers match expected schema
- Look for special characters or encoding issues

## Next Steps

After successful seed loading:
1. Run staging models: `dbt run --select staging.*`
2. Run intermediate models: `dbt run --select intermediate.*`
3. Run mart models: `dbt run --select marts.*`
4. Run tests: `dbt test`

## Success Criteria Checklist

- [ ] All 10 seed files loaded without errors
- [ ] Each table created in DBT_DEMO.RAW schema
- [ ] Row counts match expectations (see table above)
- [ ] No data type conversion warnings
- [ ] No truncation errors
- [ ] Tables accessible via `{{ ref('raw_*') }}` in downstream models
- [ ] All boolean fields stored correctly (Snowflake uses TRUE/FALSE)
- [ ] Date fields stored in correct format (YYYY-MM-DD)

## Configuration Files

### profiles.yml
```yaml
bain_capital_analytics_snowflake:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: ACCOUNTADMIN
      database: DBT_DEMO
      warehouse: COMPUTE_WH
      schema: DEV
      threads: 4
```

### dbt_project.yml (Seeds Section)
```yaml
seeds:
  bain_capital_analytics:
    +schema: raw
```

This configuration ensures all seed tables are created in the `RAW` schema, keeping raw data organized separately from transformed data.
