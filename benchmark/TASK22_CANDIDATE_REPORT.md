# Task 22: Generate Snowflake Candidate Report

## Overview

This task extracts data from all 10 Snowflake marts models and generates the candidate validation report (`candidate/report.json`) for comparison against the PostgreSQL baseline.

## Quick Start

### Automated Execution (Recommended)

**Linux/macOS:**
```bash
cd benchmark/
chmod +x generate_candidate_report.sh
./generate_candidate_report.sh
```

**Windows:**
```batch
cd benchmark\
generate_candidate_report.bat
```

### Manual Execution

```bash
cd benchmark/

# Set environment variables (if not already set)
export SNOWFLAKE_ACCOUNT='PZXTMSC-WP48482'
export SNOWFLAKE_USER='your_user'
export SNOWFLAKE_PASSWORD='your_password'

# Run the report generator
python3 generate-report.py --dialect snowflake --output candidate/report.json
```

---

## Prerequisites

### 1. Task 21 Completed
All 10 marts models must be built in Snowflake DBT_DEMO.DEV schema:
- fact_portfolio_summary
- report_portfolio_overview
- fact_portfolio_pnl
- fact_trade_activity
- report_daily_pnl
- fact_fund_performance
- fact_cashflow_waterfall
- fact_portfolio_attribution
- report_ic_dashboard
- report_lp_quarterly

### 2. Python Environment
- Python 3.7 or higher
- Dependencies installed from `requirements.txt`:
  - `snowflake-connector-python`
  - `psycopg2-binary`
  - `colorama`

### 3. Snowflake Credentials
Environment variables must be set:
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier (e.g., PZXTMSC-WP48482)
- `SNOWFLAKE_USER`: Your Snowflake username
- `SNOWFLAKE_PASSWORD`: Your Snowflake password

### 4. Database Configuration
The script connects to:
- **Warehouse**: COMPUTE_WH
- **Database**: DBT_DEMO
- **Schema**: DEV

---

## What the Script Does

### Step-by-Step Process

1. **Connect to Snowflake**
   - Validates environment variables
   - Establishes connection to DBT_DEMO.DEV schema
   - Sets up warehouse context

2. **Query Each Marts Model**
   - Executes `SELECT * FROM {model_name}` for all 10 models
   - Fetches complete result sets
   - Extracts column names and data types

3. **Apply Data Normalization**
   - **Currency columns** (amount, value, mv, pnl, commission): Round to 2 decimals
   - **Rate columns** (rate, return, pct, percentage): Round to 8 decimals
   - **Other numeric columns**: Round to 8 decimals
   - **NULL values**: Convert to empty strings

4. **Compute Deterministic Hash**
   - Sort all rows alphabetically
   - Concatenate normalized values with pipe delimiter (`|`)
   - Compute SHA256 hash of combined string
   - Ensures identical data produces identical hash

5. **Generate JSON Report**
   - Creates structured report with metadata
   - Includes row_count and data_hash for each model
   - Saves to `candidate/report.json`

---

## Expected Output

### Console Output (Success)

```
2024-01-15 14:30:00,111 - INFO - Connecting to snowflake...
2024-01-15 14:30:02,222 - INFO - Connected to Snowflake account PZXTMSC-WP48482, database DBT_DEMO, schema DEV
2024-01-15 14:30:02,333 - INFO - Processing 10 marts models...
2024-01-15 14:30:05,444 - INFO - Processing model: fact_portfolio_summary
2024-01-15 14:30:08,555 - INFO -   ✓ fact_portfolio_summary: 15247 rows, hash=a3d5f8c2b1e4d7a9...
2024-01-15 14:30:09,666 - INFO - Processing model: report_portfolio_overview
2024-01-15 14:30:11,777 - INFO -   ✓ report_portfolio_overview: 8934 rows, hash=f2a8c1d5e9b3a7c4...
2024-01-15 14:30:12,888 - INFO - Processing model: fact_portfolio_pnl
2024-01-15 14:30:15,999 - INFO -   ✓ fact_portfolio_pnl: 23456 rows, hash=b5e2a8d1c9f3a6b7...
2024-01-15 14:30:17,000 - INFO - Processing model: fact_trade_activity
2024-01-15 14:30:21,111 - INFO -   ✓ fact_trade_activity: 45123 rows, hash=c7f3b2d8e1a4c9b5...
2024-01-15 14:30:22,222 - INFO - Processing model: report_daily_pnl
2024-01-15 14:30:24,333 - INFO -   ✓ report_daily_pnl: 365 rows, hash=d8a5c2f1b9e3d7a4...
2024-01-15 14:30:25,444 - INFO - Processing model: fact_fund_performance
2024-01-15 14:30:27,555 - INFO -   ✓ fact_fund_performance: 1250 rows, hash=e9b6d3a2c8f1e5b4...
2024-01-15 14:30:28,666 - INFO - Processing model: fact_cashflow_waterfall
2024-01-15 14:30:30,777 - INFO -   ✓ fact_cashflow_waterfall: 567 rows, hash=f1c7e4b9d2a6f8c3...
2024-01-15 14:30:31,888 - INFO - Processing model: fact_portfolio_attribution
2024-01-15 14:30:33,999 - INFO -   ✓ fact_portfolio_attribution: 2345 rows, hash=a2d8f5c1b9e4d7a6...
2024-01-15 14:30:35,000 - INFO - Processing model: report_ic_dashboard
2024-01-15 14:30:36,111 - INFO -   ✓ report_ic_dashboard: 890 rows, hash=b3e9f6d2c1a8e5b7...
2024-01-15 14:30:37,222 - INFO - Processing model: report_lp_quarterly
2024-01-15 14:30:38,333 - INFO -   ✓ report_lp_quarterly: 120 rows, hash=c4f1a7e3d2b9f6c8...
2024-01-15 14:30:38,444 - INFO - Report saved to candidate/report.json
2024-01-15 14:30:38,555 - INFO - Report generation completed successfully
```

### Generated File: `candidate/report.json`

```json
{
  "generated_at": "2024-01-15T14:30:38Z",
  "dialect": "snowflake",
  "database": "DBT_DEMO",
  "schema": "DEV",
  "models": {
    "fact_portfolio_summary": {
      "row_count": 15247,
      "data_hash": "a3d5f8c2b1e4d7a9c6f3b8e1d5a9c2f7b4e8d1a6c9f2b5e8d3a7c1f4b9e6d2a8"
    },
    "report_portfolio_overview": {
      "row_count": 8934,
      "data_hash": "f2a8c1d5e9b3a7c4f1b8d2e6a9c3f7b5e1d8a4c9f6b2e7d3a8c1f5b9e4d7a2"
    },
    "fact_portfolio_pnl": {
      "row_count": 23456,
      "data_hash": "b5e2a8d1c9f3a6b7e4c2f8d5a1b9e6d3a7c4f1b8e2d9a5c6f3b7e1d4a8c2f9"
    },
    "fact_trade_activity": {
      "row_count": 45123,
      "data_hash": "c7f3b2d8e1a4c9b5e6d2a8f1c3b7e4d9a5c1f6b8e2d3a9c4f7b1e5d8a6c2f3"
    },
    "report_daily_pnl": {
      "row_count": 365,
      "data_hash": "d8a5c2f1b9e3d7a4c6f2b8e1d5a9c3f7b4e8d2a6c9f1b5e7d3a8c2f4b9e6d1"
    },
    "fact_fund_performance": {
      "row_count": 1250,
      "data_hash": "e9b6d3a2c8f1e5b4d7a3c9f2b6e8d1a5c7f4b9e2d6a3c8f1b5e9d4a7c2f6b8"
    },
    "fact_cashflow_waterfall": {
      "row_count": 567,
      "data_hash": "f1c7e4b9d2a6f8c3e5b1d7a4c9f6b2e8d3a5c1f7b9e4d6a2c8f3b5e1d9a7c4"
    },
    "fact_portfolio_attribution": {
      "row_count": 2345,
      "data_hash": "a2d8f5c1b9e4d7a6c3f8b2e5d1a9c6f4b7e1d8a3c9f2b6e4d7a1c5f8b9e3d6"
    },
    "report_ic_dashboard": {
      "row_count": 890,
      "data_hash": "b3e9f6d2c1a8e5b7d4a9c2f7b3e6d1a8c5f9b2e4d7a3c6f1b8e5d9a4c7f2b6"
    },
    "report_lp_quarterly": {
      "row_count": 120,
      "data_hash": "c4f1a7e3d2b9f6c8e5a1d7b4c9f2e6d3a8c1f5b7e9d4a6c2f8b3e1d5a9c7f4"
    }
  }
}
```

---

## Success Criteria

✅ **Script completes without errors**
- Exit code 0
- No connection timeouts
- No authentication failures

✅ **candidate/report.json is created**
- File exists in `benchmark/candidate/` directory
- File size > 100 bytes

✅ **JSON contains exactly 10 entries**
- All 10 marts models are present
- No missing or extra models

✅ **Each entry has non-zero row_count**
- All models contain data
- No empty views

✅ **Each entry has valid data_hash**
- 64-character hexadecimal string (SHA256)
- No null or empty hashes

---

## Troubleshooting

### Error: "Missing required Snowflake environment variables"

**Cause**: Environment variables not set

**Solution**:
```bash
# Linux/macOS
export SNOWFLAKE_ACCOUNT='PZXTMSC-WP48482'
export SNOWFLAKE_USER='your_user'
export SNOWFLAKE_PASSWORD='your_password'

# Windows
set SNOWFLAKE_ACCOUNT=PZXTMSC-WP48482
set SNOWFLAKE_USER=your_user
set SNOWFLAKE_PASSWORD=your_password
```

### Error: "Failed to connect to Snowflake"

**Possible Causes**:
1. Invalid credentials
2. Account identifier incorrect
3. Network connectivity issues
4. Warehouse not accessible

**Solution**:
```bash
# Test connection manually
python3 -c "
from snowflake.connector import connect
conn = connect(
    account='PZXTMSC-WP48482',
    user='your_user',
    password='your_password',
    warehouse='COMPUTE_WH',
    database='DBT_DEMO',
    schema='DEV'
)
print('Connection successful!')
conn.close()
"
```

### Error: "snowflake-connector-python not found"

**Cause**: Python dependency not installed

**Solution**:
```bash
cd benchmark/
pip3 install -r requirements.txt
```

### Error: "Database query error: Object does not exist"

**Cause**: Marts models not built in Snowflake

**Solution**:
```bash
# Run Task 21 first
cd snowflake2/
./run_models.sh

# Or manually
dbt run --profiles-dir .
```

### Error: "Model has 0 rows"

**Possible Causes**:
1. Seed data not loaded (Task 20)
2. Model logic filters all rows
3. Upstream dependencies not built

**Solution**:
```bash
# Check if seeds are loaded
cd snowflake2/
./load_seeds.sh

# Rebuild all models
dbt run --profiles-dir .

# Query specific model in Snowflake
snowsql -a PZXTMSC-WP48482 -u your_user -q "SELECT COUNT(*) FROM DBT_DEMO.DEV.fact_portfolio_summary"
```

### Error: "Output file is very small"

**Possible Causes**:
1. Script failed partway through
2. JSON generation error
3. No models processed successfully

**Solution**:
```bash
# Check the generated file
cat candidate/report.json

# Re-run with verbose logging
python3 generate-report.py --dialect snowflake --output candidate/report.json 2>&1 | tee output.log
```

---

## Validation Queries

### Check All Models Exist in Snowflake

```sql
-- Run in Snowflake SQL Worksheet
USE DATABASE DBT_DEMO;
USE SCHEMA DEV;

SELECT table_name
FROM INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DEV'
  AND table_type = 'VIEW'
  AND table_name IN (
    'FACT_PORTFOLIO_SUMMARY',
    'REPORT_PORTFOLIO_OVERVIEW',
    'FACT_PORTFOLIO_PNL',
    'FACT_TRADE_ACTIVITY',
    'REPORT_DAILY_PNL',
    'FACT_FUND_PERFORMANCE',
    'FACT_CASHFLOW_WATERFALL',
    'FACT_PORTFOLIO_ATTRIBUTION',
    'REPORT_IC_DASHBOARD',
    'REPORT_LP_QUARTERLY'
  )
ORDER BY table_name;

-- Should return 10 rows
```

### Check Row Counts for All Models

```sql
-- Individual queries
SELECT 'fact_portfolio_summary' AS model, COUNT(*) AS row_count FROM fact_portfolio_summary
UNION ALL
SELECT 'report_portfolio_overview', COUNT(*) FROM report_portfolio_overview
UNION ALL
SELECT 'fact_portfolio_pnl', COUNT(*) FROM fact_portfolio_pnl
UNION ALL
SELECT 'fact_trade_activity', COUNT(*) FROM fact_trade_activity
UNION ALL
SELECT 'report_daily_pnl', COUNT(*) FROM report_daily_pnl
UNION ALL
SELECT 'fact_fund_performance', COUNT(*) FROM fact_fund_performance
UNION ALL
SELECT 'fact_cashflow_waterfall', COUNT(*) FROM fact_cashflow_waterfall
UNION ALL
SELECT 'fact_portfolio_attribution', COUNT(*) FROM fact_portfolio_attribution
UNION ALL
SELECT 'report_ic_dashboard', COUNT(*) FROM report_ic_dashboard
UNION ALL
SELECT 'report_lp_quarterly', COUNT(*) FROM report_lp_quarterly
ORDER BY model;
```

---

## Data Normalization Rules

The script applies intelligent rounding to ensure consistent hashing across different database systems:

### Currency Columns: 2 Decimals
Keywords: `amount`, `value`, `mv`, `pnl`, `commission`

**Examples**:
- `transaction_amount: 1234.5678` → `1234.57`
- `market_value: 9999.994` → `9999.99`
- `commission_pnl: 50.256` → `50.26`

### Rate Columns: 8 Decimals
Keywords: `rate`, `return`, `pct`, `percentage`

**Examples**:
- `interest_rate: 0.045678901234` → `0.04567890`
- `annual_return: 0.124567890123` → `0.12456789`
- `fee_percentage: 0.005` → `0.00500000`

### Other Numeric Columns: 8 Decimals
All other numeric columns default to 8 decimal places.

### NULL Handling
- NULL values are converted to empty strings
- Ensures consistent hashing regardless of NULL representation

### Row Sorting
Rows are sorted deterministically by all columns (as strings) to ensure consistent hash values.

---

## Next Steps

After successfully generating the candidate report:

### Task 23: Compare Baseline vs Candidate

```bash
cd benchmark/

python3 compare.py \
  --baseline baseline/report.json \
  --candidate candidate/report.json \
  --output comparison_diff.json
```

This will:
- Load both reports
- Compare row counts for each model
- Compare data hashes for each model
- Generate detailed comparison report
- Identify any discrepancies

### Expected Results

**All Passing**:
```
✓ PASS: fact_portfolio_summary (15247 rows, hash match)
✓ PASS: report_portfolio_overview (8934 rows, hash match)
✓ PASS: fact_portfolio_pnl (23456 rows, hash match)
...
Summary: 10 passed, 0 failed
```

**With Discrepancies**:
```
✓ PASS: fact_portfolio_summary (15247 rows, hash match)
✗ FAIL: fact_portfolio_pnl (row_count mismatch: 23456 vs 23455)
...
Summary: 9 passed, 1 failed
```

---

## File Structure

After completion, your directory structure will be:

```
benchmark/
├── generate-report.py                    # Main validation script
├── generate_candidate_report.sh          # Automation script (Linux/macOS)
├── generate_candidate_report.bat         # Automation script (Windows)
├── TASK22_CANDIDATE_REPORT.md           # This documentation
├── requirements.txt                      # Python dependencies
├── baseline/
│   └── report.json                      # PostgreSQL baseline (Task 20)
└── candidate/
    └── report.json                      # Snowflake candidate (THIS TASK)
```

---

## Technical Details

### Script: `generate-report.py`

**Parameters**:
- `--dialect`: Database dialect (`postgres` or `snowflake`)
- `--output`: Output file path (default: `./report.json`)

**Hardcoded Models** (lines 35-46):
```python
MARTS_MODELS = [
    'fact_portfolio_summary',
    'report_portfolio_overview',
    'fact_portfolio_pnl',
    'fact_trade_activity',
    'report_daily_pnl',
    'fact_fund_performance',
    'fact_cashflow_waterfall',
    'fact_portfolio_attribution',
    'report_ic_dashboard',
    'report_lp_quarterly'
]
```

**Connection Details** (SnowflakeConnector class, lines 155-224):
- Account: From `SNOWFLAKE_ACCOUNT` environment variable
- User: From `SNOWFLAKE_USER` environment variable
- Password: From `SNOWFLAKE_PASSWORD` environment variable
- Warehouse: `COMPUTE_WH` (hardcoded)
- Database: `DBT_DEMO` (hardcoded)
- Schema: `DEV` (hardcoded)

**Data Processing** (DataProcessor class, lines 227-370):
- Numeric type detection
- Precision-based rounding
- Row sorting for deterministic hashing
- SHA256 hash computation

---

## Estimated Execution Time

- **Connection**: 2-5 seconds
- **Per Model Query**: 1-5 seconds (depends on row count)
- **Total Time**: 30-60 seconds for all 10 models

Larger marts (e.g., `fact_trade_activity` with 45K+ rows) may take longer.

---

## Summary

This task generates a comprehensive validation report for all Snowflake marts models. The report includes:

1. **Metadata**: Database, schema, timestamp
2. **Model Metrics**: Row count and data hash for each of 10 models
3. **Normalized Data**: Consistent numeric rounding and sorting
4. **SHA256 Hashes**: Deterministic checksums for data comparison

The candidate report enables precise comparison with the PostgreSQL baseline in Task 23, ensuring data consistency across platforms.

---

**Status**: ✅ **READY FOR EXECUTION**  
**Prerequisites**: Task 21 (Snowflake models built)  
**Output**: `candidate/report.json`  
**Next Task**: Task 23 (Compare reports)
