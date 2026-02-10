# 🚀 DBT Model Execution Guide

**Task 21: Run Snowflake Models to Build Marts**

This comprehensive guide walks through executing all dbt models to materialize the complete analytics pipeline in Snowflake.

---

## 📋 Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Model Inventory](#model-inventory)
4. [Execution Instructions](#execution-instructions)
5. [Verification](#verification)
6. [Troubleshooting](#troubleshooting)
7. [Success Criteria](#success-criteria)

---

## Overview

### 🎯 Objective
Execute all 28 translated dbt models in dependency order to materialize staging, intermediate, and marts layers in Snowflake.

### 📊 Execution Flow
```
Seeds (RAW schema)
    ↓
Staging Layer (11 models → DEV schema)
    ↓
Intermediate Layer (8 models → DEV schema)
    ↓
Marts Layer (10 models → DEV schema)
```

### 🏗️ Architecture
- **Target Database**: `DBT_DEMO`
- **Target Schema**: `DEV`
- **Materialization**: All models as **VIEWS**
- **Dependencies**: Handled automatically by dbt's DAG
- **Execution**: Single command (`dbt run`)

---

## Prerequisites

### ✅ Checklist

| Requirement | Status | Verification Command |
|-------------|--------|---------------------|
| Seeds loaded (Task 20) | ☐ | `SELECT COUNT(*) FROM DBT_DEMO.RAW.RAW_BENCHMARKS` |
| Snowflake connection active | ☐ | `snowsql -a <account> -u <user>` |
| dbt-snowflake installed | ☐ | `dbt --version` |
| Environment variables set | ☐ | `echo $SNOWFLAKE_ACCOUNT` |
| In snowflake2/ directory | ☐ | `ls dbt_project.yml` |

### 🔐 Environment Variables

Required variables for Snowflake connection:

```bash
export SNOWFLAKE_ACCOUNT='your_account_identifier'
export SNOWFLAKE_USER='your_username'
export SNOWFLAKE_PASSWORD='your_password'
```

**Note**: These are referenced in `profiles.yml` via `{{ env_var('VAR_NAME') }}`

### 📁 File Structure

```
snowflake2/
├── dbt_project.yml          # Project configuration
├── profiles.yml             # Snowflake connection config
├── models/
│   ├── staging/            # 10 staging models
│   ├── intermediate/       # 8 intermediate models
│   └── marts/              # 10 marts models
├── seeds/                  # 10 CSV seed files (already loaded)
├── run_models.sh           # Automated execution script
└── verify_models.sql       # Verification queries
```

---

## Model Inventory

### 📦 Staging Layer (10 models)

Transform raw seed data with type casting and basic business logic.

| Model | Source | Description | Row Count |
|-------|--------|-------------|-----------|
| `stg_benchmarks` | raw_benchmarks | S&P 500 and High Yield returns | ~30 |
| `stg_cashflows` | raw_cashflows | Capital calls, distributions, fees | ~25 |
| `stg_counterparties` | raw_counterparties | Active counterparties only | ~10 |
| `stg_dates` | raw_dates | Date dimension table | ~15 |
| `stg_fund_structures` | raw_fund_structures | Fund metadata and fees | 3 |
| `stg_instruments` | raw_instruments | Securities with liquidity classification | ~20 |
| `stg_portfolios` | raw_portfolios | Portfolio strategies and managers | 5 |
| `stg_positions` | raw_positions | Position snapshots with P&L | ~30 |
| `stg_trades` | raw_trades | Settled trades only | ~30 |
| `stg_valuations` | raw_valuations | Portfolio valuations | ~21 |

### 🔧 Intermediate Layer (8 models)

Business logic and enrichment transformations.

| Model | Dependencies | Description |
|-------|--------------|-------------|
| `int_benchmark_returns` | stg_benchmarks, stg_dates | Benchmark performance calculations |
| `int_cashflow_enriched` | stg_cashflows, stg_portfolios, stg_fund_structures | Cashflows with portfolio/fund context |
| `int_daily_positions` | stg_positions, stg_instruments, stg_portfolios | Positions with instrument details |
| `int_fund_nav` | stg_valuations, stg_cashflows, stg_fund_structures | Fund NAV calculations |
| `int_irr_calculations` | stg_cashflows | IRR and MOIC metrics |
| `int_portfolio_attribution` | stg_positions, stg_benchmarks, stg_portfolios | Performance attribution |
| `int_trade_enriched` | stg_trades, stg_instruments, stg_counterparties, stg_portfolios | Trades with full context |
| `int_valuation_enriched` | stg_valuations, stg_portfolios, stg_fund_structures | Valuations with fund details |

### 📊 Marts Layer (10 models)

Final analytics tables for reporting and dashboards.

| Model | Type | Pipeline | Description |
|-------|------|----------|-------------|
| `fact_cashflow_waterfall` | Fact | C | LP cashflow analysis |
| `fact_fund_performance` | Fact | C | Fund-level returns and IRR |
| `fact_portfolio_attribution` | Fact | B | Performance attribution analysis |
| `fact_portfolio_pnl` | Fact | A | Daily P&L by portfolio |
| `fact_portfolio_summary` | Fact | A | Portfolio position summaries |
| `fact_trade_activity` | Fact | B | Trade execution metrics |
| `report_daily_pnl` | Report | A | Daily P&L dashboard |
| `report_ic_dashboard` | Report | B | Investment Committee report |
| `report_lp_quarterly` | Report | C | Limited Partner quarterly report |
| `report_portfolio_overview` | Report | A | Portfolio overview dashboard |

**Pipeline Legend:**
- **Pipeline A**: Position & P&L tracking
- **Pipeline B**: Trading & attribution
- **Pipeline C**: Fund performance & LP reporting

---

## Execution Instructions

### 🎬 Option 1: Automated Script (Recommended)

The automated script provides progress monitoring, error handling, and validation.

```bash
# From project root, navigate to snowflake2/
cd snowflake2/

# Make script executable
chmod +x run_models.sh

# Execute
./run_models.sh
```

**What the script does:**
1. ✅ Validates environment variables
2. ✅ Checks dbt installation
3. ✅ Verifies directory structure
4. ✅ Counts model files
5. ✅ Executes `dbt run --profiles-dir .`
6. ✅ Provides colored output and summary

### 🔧 Option 2: Manual Execution

For direct control or debugging:

```bash
# From project root, navigate to snowflake2/
cd snowflake2/

# Run all models
dbt run --profiles-dir .

# Alternative: Run with verbose logging
dbt run --profiles-dir . --debug

# Alternative: Run specific layer
dbt run --select staging --profiles-dir .
dbt run --select intermediate --profiles-dir .
dbt run --select marts --profiles-dir .
```

### 📊 Expected Output

```
Running with dbt=1.x.x
Found 28 models, 0 tests, 0 snapshots, 0 analyses, 0 macros, 0 operations, 10 seed files, 0 sources

[INFO] Concurrency: 4 threads (target='dev')

1 of 28 START sql view model DEV.stg_benchmarks ..................... [RUN]
1 of 28 OK created sql view model DEV.stg_benchmarks ................. [SUCCESS 1 in 0.45s]
2 of 28 START sql view model DEV.stg_cashflows ....................... [RUN]
...
28 of 28 OK created sql view model DEV.report_portfolio_overview ..... [SUCCESS 1 in 0.52s]

Finished running 28 view models in 0:00:15.32

Completed successfully
```

### ⏱️ Execution Time

- **Staging**: ~5-8 seconds (10 simple transformations)
- **Intermediate**: ~10-15 seconds (8 enrichment models)
- **Marts**: ~15-20 seconds (10 aggregation models)
- **Total**: ~30-45 seconds

---

## Verification

### 🔍 Automated Verification

Run the comprehensive verification script:

```bash
# From Snowflake SQL editor or snowsql
USE DATABASE DBT_DEMO;
USE SCHEMA DEV;

-- Option 1: Run entire script
!source verify_models.sql

-- Option 2: Copy/paste sections individually
-- (see verify_models.sql for 8 verification sections)
```

### 📋 Verification Sections

1. **View Existence Check**: Confirms all 28 views created
2. **Detailed View List**: Metadata for each view
3. **Staging Layer Row Counts**: Verify data propagation
4. **Intermediate Layer Row Counts**: Check transformations
5. **Marts Layer Row Counts**: Validate aggregations
6. **Data Quality Checks**: Ensure no empty views
7. **Sample Data**: Preview key marts
8. **Summary Report**: Final checklist

### ✅ Quick Manual Checks

```sql
-- Check total model count
SELECT COUNT(*) AS total_views
FROM DBT_DEMO.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DEV'
  AND table_type = 'VIEW';
-- Expected: 28

-- List all marts
SELECT table_name
FROM DBT_DEMO.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DEV'
  AND (table_name LIKE 'FACT_%' OR table_name LIKE 'REPORT_%')
ORDER BY table_name;
-- Expected: 10 results

-- Sample a key report
SELECT *
FROM DBT_DEMO.DEV.REPORT_PORTFOLIO_OVERVIEW
LIMIT 5;
```

---

## Troubleshooting

### ❌ Common Errors

#### 1. **Environment Variable Not Set**

**Error:**
```
Env var required but not provided: 'SNOWFLAKE_ACCOUNT'
```

**Solution:**
```bash
export SNOWFLAKE_ACCOUNT='your_account'
export SNOWFLAKE_USER='your_user'
export SNOWFLAKE_PASSWORD='your_password'
```

#### 2. **Connection Failed**

**Error:**
```
250001 (08001): Failed to connect to DB. Verify the account name is correct
```

**Solution:**
- Verify account identifier: `echo $SNOWFLAKE_ACCOUNT`
- Check credentials are correct
- Ensure Snowflake account is active
- Test connection: `snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER`

#### 3. **Seed Tables Not Found**

**Error:**
```
Database Error in model stg_benchmarks
  Object 'DBT_DEMO.RAW.RAW_BENCHMARKS' does not exist
```

**Solution:**
```bash
# Seeds must be loaded first (Task 20)
dbt seed --profiles-dir .

# Verify seeds exist
SELECT * FROM DBT_DEMO.RAW.RAW_BENCHMARKS LIMIT 1;
```

#### 4. **SQL Compilation Error**

**Error:**
```
Compilation Error in model int_daily_positions
  syntax error line 15 at position 42 unexpected 'EXTRACT'
```

**Solution:**
- Review the model's SQL syntax
- Check for BigQuery-specific functions that need Snowflake translation
- Common issues:
  - `EXTRACT(DATE FROM ...)` → `CAST(... AS DATE)`
  - `TRUE`/`FALSE` → `1`/`0` or `'true'`/`'false'`
  - `||` (concat) → `CONCAT()` or `||` (both work in Snowflake)

#### 5. **Dependency Not Found**

**Error:**
```
Model 'int_portfolio_attribution' depends on 'stg_benchmarks' which was not found
```

**Solution:**
- Ensure all staging models completed successfully
- Run staging layer first: `dbt run --select staging --profiles-dir .`
- Then run downstream: `dbt run --select int_portfolio_attribution+ --profiles-dir .`

### 🔄 Re-running Failed Models

```bash
# Re-run only failed models
dbt run --select result:error --profiles-dir .

# Re-run a specific model and its dependents
dbt run --select stg_benchmarks+ --profiles-dir .

# Re-run everything from a specific model onward
dbt run --select stg_benchmarks+ --profiles-dir .
```

### 🐛 Debug Mode

For detailed logging:

```bash
dbt run --profiles-dir . --debug > dbt_run.log 2>&1
```

---

## Success Criteria

### ✅ Validation Checklist

| Criteria | Verification Method | Expected Result |
|----------|---------------------|-----------------|
| All 28 models execute successfully | dbt run output | "Completed successfully" |
| No SQL syntax errors | dbt run log | 0 errors |
| All 10 marts views exist | `verify_models.sql` Section 1 | 28 total views |
| Views materialized correctly | Snowflake INFORMATION_SCHEMA | All table_type = 'VIEW' |
| Views contain data | `verify_models.sql` Section 6 | 0 empty views |
| Staging layer complete | `verify_models.sql` Section 3 | 10 models with data |
| Intermediate layer complete | `verify_models.sql` Section 4 | 8 models with data |
| Marts layer complete | `verify_models.sql` Section 5 | 10 models with data |
| Dependencies respected | dbt DAG order | No dependency errors |
| Target schema correct | Snowflake query | All in DBT_DEMO.DEV |

### 🎯 Final Validation Query

```sql
-- Run this to confirm complete success
SELECT 
    CASE 
        WHEN COUNT(*) = 28 THEN '✅ SUCCESS: All 28 models created'
        ELSE '❌ FAILURE: Only ' || COUNT(*) || ' models created'
    END AS validation_status
FROM DBT_DEMO.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DEV'
  AND table_type = 'VIEW'
  AND table_name IN (
    'STG_BENCHMARKS', 'STG_CASHFLOWS', 'STG_COUNTERPARTIES', 'STG_DATES',
    'STG_FUND_STRUCTURES', 'STG_INSTRUMENTS', 'STG_PORTFOLIOS', 'STG_POSITIONS',
    'STG_TRADES', 'STG_VALUATIONS', 'INT_BENCHMARK_RETURNS', 'INT_CASHFLOW_ENRICHED',
    'INT_DAILY_POSITIONS', 'INT_FUND_NAV', 'INT_IRR_CALCULATIONS',
    'INT_PORTFOLIO_ATTRIBUTION', 'INT_TRADE_ENRICHED', 'INT_VALUATION_ENRICHED',
    'FACT_CASHFLOW_WATERFALL', 'FACT_FUND_PERFORMANCE', 'FACT_PORTFOLIO_ATTRIBUTION',
    'FACT_PORTFOLIO_PNL', 'FACT_PORTFOLIO_SUMMARY', 'FACT_TRADE_ACTIVITY',
    'REPORT_DAILY_PNL', 'REPORT_IC_DASHBOARD', 'REPORT_LP_QUARTERLY',
    'REPORT_PORTFOLIO_OVERVIEW'
  );
```

---

## 📊 Execution Timeline

```
┌─────────────────────────────────────────────────────────────┐
│                  DBT RUN EXECUTION FLOW                     │
└─────────────────────────────────────────────────────────────┘

[00:00 - 00:08]  📦 STAGING LAYER (10 models)
                     ├─ stg_benchmarks
                     ├─ stg_cashflows
                     ├─ stg_counterparties
                     ├─ stg_dates
                     ├─ stg_fund_structures
                     ├─ stg_instruments
                     ├─ stg_portfolios
                     ├─ stg_positions
                     ├─ stg_trades
                     └─ stg_valuations

[00:08 - 00:23]  🔧 INTERMEDIATE LAYER (8 models)
                     ├─ int_benchmark_returns
                     ├─ int_cashflow_enriched
                     ├─ int_daily_positions
                     ├─ int_fund_nav
                     ├─ int_irr_calculations
                     ├─ int_portfolio_attribution
                     ├─ int_trade_enriched
                     └─ int_valuation_enriched

[00:23 - 00:45]  📊 MARTS LAYER (10 models)
                     ├─ fact_cashflow_waterfall
                     ├─ fact_fund_performance
                     ├─ fact_portfolio_attribution
                     ├─ fact_portfolio_pnl
                     ├─ fact_portfolio_summary
                     ├─ fact_trade_activity
                     ├─ report_daily_pnl
                     ├─ report_ic_dashboard
                     ├─ report_lp_quarterly
                     └─ report_portfolio_overview

[00:45]          ✅ COMPLETE
```

---

## 🔜 Next Steps

After successful execution:

1. **Run dbt Tests** (Task 22?):
   ```bash
   dbt test --profiles-dir .
   ```

2. **Generate Documentation**:
   ```bash
   dbt docs generate --profiles-dir .
   dbt docs serve
   ```

3. **Query Marts**:
   ```sql
   -- Portfolio overview
   SELECT * FROM DBT_DEMO.DEV.REPORT_PORTFOLIO_OVERVIEW;
   
   -- IC Dashboard
   SELECT * FROM DBT_DEMO.DEV.REPORT_IC_DASHBOARD;
   
   -- LP Quarterly Report
   SELECT * FROM DBT_DEMO.DEV.REPORT_LP_QUARTERLY;
   ```

4. **Generate Candidate Report** (Task 22):
   - Compare BigQuery vs Snowflake outputs
   - Document translation quality
   - Identify any discrepancies

---

## 📚 Additional Resources

- **dbt Documentation**: https://docs.getdbt.com/
- **Snowflake SQL Reference**: https://docs.snowflake.com/sql-reference
- **Project Structure**: See `dbt_project.yml`
- **Model Lineage**: Generate docs for visual DAG

---

## 🆘 Support

If issues persist:

1. Review dbt logs in `target/run/`
2. Check compiled SQL in `target/compiled/`
3. Verify seed data loaded correctly
4. Test Snowflake connection independently
5. Review model dependencies in schema files

---

**Document Version**: 1.0  
**Last Updated**: Task 21 Execution  
**Status**: ✅ Ready for Execution
