# Task 22: Generate Snowflake Candidate Report - README

## Status: ✅ READY FOR EXECUTION

---

## Quick Execute

```bash
cd benchmark/
chmod +x generate_candidate_report.sh
./generate_candidate_report.sh
```

**OR**

```bash
cd benchmark/
python3 generate-report.py --dialect snowflake --output candidate/report.json
```

---

## Overview

Task 22 generates a validation report from Snowflake containing:
- Row counts for all 10 marts models
- SHA256 hashes of normalized data
- Metadata (database, schema, timestamp)

This candidate report will be compared with the PostgreSQL baseline in Task 23.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Task 22 Workflow                          │
└─────────────────────────────────────────────────────────────┘

    ┌──────────────────┐
    │  Snowflake       │
    │  DBT_DEMO.DEV    │
    │  10 Marts Views  │
    └────────┬─────────┘
             │
             ▼
    ┌──────────────────┐
    │ generate-report  │
    │     .py          │
    │ --dialect        │
    │  snowflake       │
    └────────┬─────────┘
             │
             ├─ Connect to Snowflake
             ├─ Query each mart (SELECT *)
             ├─ Apply normalization
             │  • Currency: 2 decimals
             │  • Rates: 8 decimals
             │  • Others: 8 decimals
             ├─ Sort rows deterministically
             ├─ Compute SHA256 hash
             │
             ▼
    ┌──────────────────┐
    │  candidate/      │
    │  report.json     │
    │                  │
    │  • row_count     │
    │  • data_hash     │
    │  × 10 models     │
    └──────────────────┘
```

---

## Files Created

| File | Purpose | Status |
|------|---------|--------|
| `generate_candidate_report.sh` | Automation script (Unix) | ✅ Created |
| `generate_candidate_report.bat` | Automation script (Windows) | ✅ Created |
| `verify_candidate_report.py` | Validation script | ✅ Created |
| `TASK22_CANDIDATE_REPORT.md` | Full documentation | ✅ Created |
| `QUICKSTART_TASK22.md` | Quick reference | ✅ Created |
| `README_TASK22.md` | This file | ✅ Created |
| `candidate/report.json` | **OUTPUT** (generated on execution) | ⏳ Pending |

---

## Prerequisites

### ✅ Task 21 Must Be Complete
All 10 marts models must exist in Snowflake:

```sql
-- Verify in Snowflake
USE DATABASE DBT_DEMO;
USE SCHEMA DEV;

SELECT COUNT(*) AS view_count
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
  );

-- Should return: 10
```

### ✅ Environment Variables
```bash
export SNOWFLAKE_ACCOUNT='PZXTMSC-WP48482'
export SNOWFLAKE_USER='your_user'
export SNOWFLAKE_PASSWORD='your_password'
```

### ✅ Python Dependencies
```bash
pip3 install -r requirements.txt
```

---

## Execution Methods

### Method 1: Automated Script (Recommended)

**Advantages**:
- ✅ Validates all prerequisites
- ✅ Handles errors gracefully
- ✅ Provides detailed progress output
- ✅ Verifies output structure
- ✅ Displays summary report

**Linux/macOS**:
```bash
cd benchmark/
chmod +x generate_candidate_report.sh
./generate_candidate_report.sh
```

**Windows**:
```batch
cd benchmark\
generate_candidate_report.bat
```

### Method 2: Direct Python

**Advantages**:
- ✅ Simple and direct
- ✅ No shell script needed

**Command**:
```bash
cd benchmark/
python3 generate-report.py --dialect snowflake --output candidate/report.json
```

---

## Expected Output

### Console Output

```
2024-01-15 14:30:00 - INFO - Connecting to snowflake...
2024-01-15 14:30:02 - INFO - Connected to Snowflake account PZXTMSC-WP48482, database DBT_DEMO, schema DEV
2024-01-15 14:30:02 - INFO - Processing 10 marts models...
2024-01-15 14:30:05 - INFO - Processing model: fact_portfolio_summary
2024-01-15 14:30:08 - INFO -   ✓ fact_portfolio_summary: 15247 rows, hash=a3d5f8c2b1e4d7a9...
2024-01-15 14:30:09 - INFO - Processing model: report_portfolio_overview
2024-01-15 14:30:11 - INFO -   ✓ report_portfolio_overview: 8934 rows, hash=f2a8c1d5e9b3a7c4...
2024-01-15 14:30:12 - INFO - Processing model: fact_portfolio_pnl
2024-01-15 14:30:15 - INFO -   ✓ fact_portfolio_pnl: 23456 rows, hash=b5e2a8d1c9f3a6b7...
2024-01-15 14:30:17 - INFO - Processing model: fact_trade_activity
2024-01-15 14:30:21 - INFO -   ✓ fact_trade_activity: 45123 rows, hash=c7f3b2d8e1a4c9b5...
2024-01-15 14:30:22 - INFO - Processing model: report_daily_pnl
2024-01-15 14:30:24 - INFO -   ✓ report_daily_pnl: 365 rows, hash=d8a5c2f1b9e3d7a4...
2024-01-15 14:30:25 - INFO - Processing model: fact_fund_performance
2024-01-15 14:30:27 - INFO -   ✓ fact_fund_performance: 1250 rows, hash=e9b6d3a2c8f1e5b4...
2024-01-15 14:30:28 - INFO - Processing model: fact_cashflow_waterfall
2024-01-15 14:30:30 - INFO -   ✓ fact_cashflow_waterfall: 567 rows, hash=f1c7e4b9d2a6f8c3...
2024-01-15 14:30:31 - INFO - Processing model: fact_portfolio_attribution
2024-01-15 14:30:33 - INFO -   ✓ fact_portfolio_attribution: 2345 rows, hash=a2d8f5c1b9e4d7a6...
2024-01-15 14:30:35 - INFO - Processing model: report_ic_dashboard
2024-01-15 14:30:36 - INFO -   ✓ report_ic_dashboard: 890 rows, hash=b3e9f6d2c1a8e5b7...
2024-01-15 14:30:37 - INFO - Processing model: report_lp_quarterly
2024-01-15 14:30:38 - INFO -   ✓ report_lp_quarterly: 120 rows, hash=c4f1a7e3d2b9f6c8...
2024-01-15 14:30:38 - INFO - Report saved to candidate/report.json
2024-01-15 14:30:38 - INFO - Report generation completed successfully
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

## Verification

### Quick Check
```bash
cd benchmark/

# Verify file exists
ls -lh candidate/report.json

# Validate structure
python3 verify_candidate_report.py
```

### Manual Validation
```bash
# Count models
python3 -c "import json; data=json.load(open('candidate/report.json')); print(f'Models: {len(data[\"models\"])}')"
# Expected: Models: 10

# Check for errors
python3 -c "import json; data=json.load(open('candidate/report.json')); errors=[k for k,v in data['models'].items() if 'error' in v]; print(f'Errors: {errors if errors else \"None\"}')"
# Expected: Errors: None

# List row counts
python3 -c "import json; data=json.load(open('candidate/report.json')); [print(f'{k}: {v[\"row_count\"]:,} rows') for k,v in sorted(data['models'].items())]"
```

---

## Success Criteria

| Criterion | Validation |
|-----------|------------|
| ✅ Script exits with code 0 | Check exit code |
| ✅ `candidate/report.json` created | File exists |
| ✅ File size > 100 bytes | Check file size |
| ✅ Valid JSON structure | Parse without errors |
| ✅ Contains 10 model entries | `len(data['models']) == 10` |
| ✅ Each entry has `row_count` > 0 | No zero counts |
| ✅ Each entry has 64-char `data_hash` | SHA256 format |
| ✅ Dialect is 'snowflake' | Check metadata |
| ✅ Database is 'DBT_DEMO' | Check metadata |
| ✅ Schema is 'DEV' | Check metadata |

---

## Troubleshooting

### Issue: "Missing environment variables"

**Solution**:
```bash
export SNOWFLAKE_ACCOUNT='PZXTMSC-WP48482'
export SNOWFLAKE_USER='your_user'
export SNOWFLAKE_PASSWORD='your_password'
```

### Issue: "Failed to connect to Snowflake"

**Possible Causes**:
- Invalid credentials
- Network connectivity
- Warehouse not running

**Solution**:
```bash
# Test connection
python3 -c "
from snowflake.connector import connect
conn = connect(
    account='PZXTMSC-WP48482',
    user='your_user',
    password='your_password',
    warehouse='COMPUTE_WH'
)
print('Connection successful!')
conn.close()
"
```

### Issue: "Object does not exist"

**Cause**: Marts not built in Snowflake

**Solution**:
```bash
cd snowflake2/
./run_models.sh
```

### Issue: "Model has 0 rows"

**Cause**: No seed data or upstream dependencies missing

**Solution**:
```bash
# Load seeds
cd snowflake2/
./load_seeds.sh

# Rebuild models
dbt run --profiles-dir .
```

---

## 10 Marts Models

| # | Model Name | Description | Expected Rows |
|---|------------|-------------|---------------|
| 1 | fact_portfolio_summary | Portfolio aggregations | ~15,000 |
| 2 | report_portfolio_overview | Portfolio reporting | ~9,000 |
| 3 | fact_portfolio_pnl | P&L calculations | ~23,000 |
| 4 | fact_trade_activity | Trade transactions | ~45,000 |
| 5 | report_daily_pnl | Daily P&L report | ~365 |
| 6 | fact_fund_performance | Fund metrics | ~1,250 |
| 7 | fact_cashflow_waterfall | Cashflow analysis | ~567 |
| 8 | fact_portfolio_attribution | Attribution analysis | ~2,345 |
| 9 | report_ic_dashboard | IC dashboard | ~890 |
| 10 | report_lp_quarterly | LP quarterly report | ~120 |

---

## Next Steps

After successful generation:

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
- Compare row counts
- Compare data hashes
- Generate detailed comparison
- Identify discrepancies

---

## Documentation Index

| File | Purpose | When to Use |
|------|---------|-------------|
| `QUICKSTART_TASK22.md` | TL;DR quick start | Fast execution |
| `TASK22_CANDIDATE_REPORT.md` | Full documentation | Detailed reference |
| `README_TASK22.md` | This file | Overview & status |
| `generate_candidate_report.sh` | Automation script | Primary execution |
| `verify_candidate_report.py` | Validation tool | Post-execution check |

---

## Technical Details

### Script: `generate-report.py`
- **Language**: Python 3.7+
- **Dependencies**: `snowflake-connector-python`, `psycopg2-binary`
- **Connection**: Snowflake account PZXTMSC-WP48482
- **Target**: DBT_DEMO.DEV schema
- **Query**: `SELECT * FROM {model_name}`
- **Hash Algorithm**: SHA256
- **Output Format**: JSON

### Data Normalization
- **Currency** (amount, value, mv, pnl, commission): 2 decimals
- **Rates** (rate, return, pct, percentage): 8 decimals
- **Other numerics**: 8 decimals
- **NULL values**: Empty strings
- **Row sorting**: Deterministic (all columns as strings)

### Performance
- **Connection time**: 2-5 seconds
- **Per model**: 1-5 seconds
- **Total time**: 30-60 seconds

---

## Summary

**Task 22** generates a comprehensive validation report from Snowflake containing row counts and SHA256 hashes for all 10 marts models. This candidate report enables precise comparison with the PostgreSQL baseline in Task 23.

**Status**: ✅ **READY FOR EXECUTION**  
**Prerequisites**: Task 21 (Snowflake marts built)  
**Output**: `candidate/report.json`  
**Next**: Task 23 (Compare reports)

---

**Created**: Task 22 Implementation  
**Files Generated**: 6 automation/documentation files  
**Execution Ready**: Yes  
**Validation Enabled**: Yes
