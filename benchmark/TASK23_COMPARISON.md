# Task 23: Compare Baseline vs Candidate Outputs

## Overview

Task 23 compares PostgreSQL baseline data against Snowflake candidate data to validate that the SQL translation produces identical outputs for all 10 marts models.

## Quick Start

### Automated Execution (Recommended)

**Linux/macOS:**
```bash
cd benchmark/
chmod +x run_task23.sh
./run_task23.sh
```

**Windows:**
```cmd
cd benchmark
run_task23.bat
```

**Python (All Platforms):**
```bash
cd benchmark/
python3 run_comparison_task23.py
```

## Prerequisites

### 1. Environment Variables

Both PostgreSQL and Snowflake credentials must be configured:

**PostgreSQL:**
```bash
export DBT_PG_USER=postgres
export DBT_PG_PASSWORD=your_password
export DBT_PG_DBNAME=bain_analytics
```

**Snowflake:**
```bash
export SNOWFLAKE_ACCOUNT=PZXTMSC-WP48482  # Or your account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
```

**Windows (CMD):**
```cmd
set DBT_PG_USER=postgres
set DBT_PG_PASSWORD=your_password
set DBT_PG_DBNAME=bain_analytics
set SNOWFLAKE_ACCOUNT=PZXTMSC-WP48482
set SNOWFLAKE_USER=your_user
set SNOWFLAKE_PASSWORD=your_password
```

### 2. Database Requirements

- **PostgreSQL**: Running at `localhost:5433` with all 10 marts built
- **Snowflake**: Account accessible with all 10 marts built
- **dbt Models**: All models materialized in both databases

### 3. Python Dependencies

```bash
cd benchmark/
pip install -r requirements.txt
```

Required packages:
- `psycopg2-binary` (PostgreSQL driver)
- `snowflake-connector-python` (Snowflake driver)
- `colorama` (optional, for colored output)

## What This Task Does

Task 23 performs a complete comparison workflow:

### Step 1: Generate Baseline Report (PostgreSQL)
- Connects to PostgreSQL database
- Queries all 10 marts models
- Computes row counts and SHA256 data hashes
- Saves results to `baseline/report.json`

### Step 2: Generate Candidate Report (Snowflake)
- Connects to Snowflake database
- Queries all 10 marts models
- Computes row counts and SHA256 data hashes
- Saves results to `candidate/report.json`

### Step 3: Compare Reports
- Loads both baseline and candidate reports
- Compares row counts (must be exact)
- Compares data hashes (must be exact)
- Generates detailed comparison results
- Saves to `comparison_diff.json`

### Step 4: Generate Summary
- Displays human-readable comparison results
- Shows PASS/FAIL status for each model
- Identifies specific issues (row count or hash mismatches)

## Expected Output

### Success Case (All Models Pass)

```
================================================================================
Task 23: Compare Baseline vs Candidate Outputs
================================================================================

[Step 1] Checking environment variables
--------------------------------------------------------------------------------
✓ All required environment variables are set

[Step 2] Generating baseline report from PostgreSQL
--------------------------------------------------------------------------------
Running: python generate-report.py --dialect postgres --output baseline/report.json

2024-01-15 10:23:45,123 - INFO - Connecting to postgres...
2024-01-15 10:23:46,456 - INFO - Connected to Postgres at localhost:5433/bain_analytics
2024-01-15 10:23:46,789 - INFO - Processing 10 marts models...
2024-01-15 10:23:47,012 - INFO - Processing model: fact_portfolio_summary
2024-01-15 10:23:49,456 - INFO -   ✓ fact_portfolio_summary: 15247 rows, hash=a3d5f8c2...
...
2024-01-15 10:25:12,890 - INFO - Report saved to baseline/report.json
✓ Baseline report generated

[Step 3] Verifying baseline report
--------------------------------------------------------------------------------
✓ Baseline report verified: Valid (10 models, 3245 bytes)

[Step 4] Generating candidate report from Snowflake
--------------------------------------------------------------------------------
Running: python generate-report.py --dialect snowflake --output candidate/report.json

2024-01-15 10:25:15,123 - INFO - Connecting to snowflake...
2024-01-15 10:25:18,456 - INFO - Connected to Snowflake account PZXTMSC-WP48482...
2024-01-15 10:25:18,789 - INFO - Processing 10 marts models...
...
✓ Candidate report generated

[Step 5] Verifying candidate report
--------------------------------------------------------------------------------
✓ Candidate report verified: Valid (10 models, 3198 bytes)

[Step 6] Running comparison between baseline and candidate
--------------------------------------------------------------------------------
Running: python compare.py --baseline baseline/report.json --candidate candidate/report.json --output comparison_diff.json

================================================================================
Validation Report Comparison
================================================================================

Baseline: postgres (bain_analytics)
  Generated: 2024-01-15T10:25:12Z

Candidate: snowflake (DBT_DEMO.DEV)
  Generated: 2024-01-15T10:26:30Z

--------------------------------------------------------------------------------
Results:
--------------------------------------------------------------------------------

✓ PASS: fact_portfolio_summary (15247 rows, hash match)
✓ PASS: report_portfolio_overview (8934 rows, hash match)
✓ PASS: fact_portfolio_pnl (23456 rows, hash match)
✓ PASS: fact_trade_activity (45123 rows, hash match)
✓ PASS: report_daily_pnl (365 rows, hash match)
✓ PASS: fact_fund_performance (1250 rows, hash match)
✓ PASS: fact_cashflow_waterfall (567 rows, hash match)
✓ PASS: fact_portfolio_attribution (2345 rows, hash match)
✓ PASS: report_ic_dashboard (890 rows, hash match)
✓ PASS: report_lp_quarterly (120 rows, hash match)

--------------------------------------------------------------------------------
Summary:
  Total models: 10
  Passed: 10
  Failed: 0
  Missing: 0
  Extra: 0
================================================================================

✓ Comparison completed (exit code: 0)

[Step 7] Generating summary report
--------------------------------------------------------------------------------

================================================================================
Task 23 Summary Report
================================================================================
Comparison Details:
  Baseline: postgres (bain_analytics.public)
  Candidate: snowflake (DBT_DEMO.DEV)
  Comparison timestamp: 2024-01-15T10:27:45Z

Results Summary:
  Total models: 10
  ✓ Passed: 10
  ✗ Failed: 0
  ⚠ Missing: 0
  ⚠ Extra: 0

Detailed Results:

  PASSED Models:
    ✓ fact_portfolio_summary (15247 rows)
    ✓ report_portfolio_overview (8934 rows)
    ✓ fact_portfolio_pnl (23456 rows)
    ✓ fact_trade_activity (45123 rows)
    ✓ report_daily_pnl (365 rows)
    ✓ fact_fund_performance (1250 rows)
    ✓ fact_cashflow_waterfall (567 rows)
    ✓ fact_portfolio_attribution (2345 rows)
    ✓ report_ic_dashboard (890 rows)
    ✓ report_lp_quarterly (120 rows)

================================================================================
✓ SUCCESS: All 10 marts models PASSED validation!
  Row counts and data hashes match exactly between baseline and candidate.
================================================================================

Task 23 execution completed!
Review the comparison results above.

Generated files:
  - baseline/report.json (PostgreSQL ground truth)
  - candidate/report.json (Snowflake outputs)
  - comparison_diff.json (detailed comparison results)
```

### Failure Case (Some Models Fail)

If models fail validation, the output will show:

```
✗ FAIL: fact_trade_activity
        row_count mismatch: 45123 vs 45120
✗ FAIL: fact_portfolio_pnl
        data_hash mismatch

--------------------------------------------------------------------------------
Summary:
  Total models: 10
  Passed: 8
  Failed: 2
  Missing: 0
  Extra: 0
================================================================================

✗ FAILURE: Some models did not pass validation.
  Review the failures above and proceed to Task #34 for fixes.
================================================================================
```

## Understanding Failures

### 1. Row Count Mismatch

**Symptom:**
```
✗ FAIL: fact_trade_activity
        row_count mismatch: 45123 vs 45120
```

**Possible Causes:**
- WHERE clause differences
- Boolean literal issues (true vs TRUE vs 1)
- Missing or extra rows due to JOIN differences
- Date/timestamp filtering differences

**Investigation Steps:**
1. Check the model's WHERE clauses
2. Look for boolean comparisons
3. Review JOIN conditions
4. Compare date functions (EXTRACT, DATE_TRUNC, etc.)

### 2. Hash Mismatch (Same Row Count)

**Symptom:**
```
✗ FAIL: fact_portfolio_pnl
        data_hash mismatch
```

**Possible Causes:**
- Numeric precision/rounding differences
- Date formatting differences
- NULL handling variations
- String case differences
- Timestamp timezone issues

**Investigation Steps:**
1. Review numeric columns and rounding logic
2. Check date arithmetic and formatting
3. Look for COALESCE/NULLIF usage
4. Compare string functions (UPPER, LOWER, TRIM)

### 3. Missing Model

**Symptom:**
```
⚠ MISSING: report_lp_quarterly (not found in candidate)
```

**Possible Causes:**
- Model not built in Snowflake
- dbt run failed for this model
- Model excluded from Snowflake project

**Investigation Steps:**
1. Run `dbt run --select report_lp_quarterly` in snowflake2/
2. Check dbt logs for build errors
3. Verify model exists in snowflake2/models/marts/

## Files Generated

### baseline/report.json

PostgreSQL ground truth containing:
```json
{
  "generated_at": "2024-01-15T10:25:12Z",
  "dialect": "postgres",
  "database": "bain_analytics",
  "schema": "public",
  "models": {
    "fact_portfolio_summary": {
      "row_count": 15247,
      "data_hash": "a3d5f8c2b1e4d7a9c6f3b8e1d5a9c2f7e4b1c8d6a3f9e2b5d8c1f4a7e3b6d9c2"
    },
    ...
  }
}
```

### candidate/report.json

Snowflake outputs containing same structure as baseline.

### comparison_diff.json

Detailed comparison results:
```json
{
  "comparison_timestamp": "2024-01-15T10:27:45Z",
  "baseline_metadata": {
    "dialect": "postgres",
    "database": "bain_analytics",
    "schema": "public",
    "generated_at": "2024-01-15T10:25:12Z"
  },
  "candidate_metadata": {
    "dialect": "snowflake",
    "database": "DBT_DEMO",
    "schema": "DEV",
    "generated_at": "2024-01-15T10:26:30Z"
  },
  "results": {
    "passed": [
      {
        "model": "fact_portfolio_summary",
        "passed": true,
        "baseline_row_count": 15247,
        "candidate_row_count": 15247,
        "baseline_hash": "a3d5f8c2...",
        "candidate_hash": "a3d5f8c2...",
        "issues": []
      }
    ],
    "failed": [],
    "missing": [],
    "extra": []
  },
  "summary": {
    "total_models": 10,
    "passed": 10,
    "failed": 0,
    "missing": 0,
    "extra": 0
  }
}
```

## Manual Execution Steps

If you prefer to run steps individually:

### Step 1: Generate Baseline Report
```bash
cd benchmark/
python3 generate-report.py --dialect postgres --output baseline/report.json
```

### Step 2: Generate Candidate Report
```bash
python3 generate-report.py --dialect snowflake --output candidate/report.json
```

### Step 3: Compare Reports
```bash
python3 compare.py \
  --baseline baseline/report.json \
  --candidate candidate/report.json \
  --output comparison_diff.json
```

## Troubleshooting

### Issue: Missing Environment Variables

**Error:**
```
✗ Missing required environment variables: DBT_PG_USER, DBT_PG_PASSWORD
```

**Solution:**
Set all required environment variables (see Prerequisites section).

### Issue: Cannot Connect to PostgreSQL

**Error:**
```
Failed to connect to Postgres: connection refused
```

**Solution:**
1. Ensure PostgreSQL container is running:
   ```bash
   cd postgres/
   docker-compose ps
   docker-compose up -d  # if not running
   ```
2. Verify port 5433 is accessible
3. Check credentials

### Issue: Cannot Connect to Snowflake

**Error:**
```
Failed to connect to Snowflake: Invalid account identifier
```

**Solution:**
1. Verify SNOWFLAKE_ACCOUNT format (e.g., PZXTMSC-WP48482)
2. Check credentials
3. Ensure network connectivity

### Issue: Model Not Found

**Error:**
```
Database query error: Object 'fact_portfolio_summary' does not exist
```

**Solution:**
Build the missing model:
```bash
cd postgres/  # or snowflake2/
dbt run --select fact_portfolio_summary
```

### Issue: Zero Row Count

**Error:**
```
✗ FAIL: fact_trade_activity
        row_count mismatch: 0 vs 45123
```

**Solution:**
1. Load seed data:
   ```bash
   cd postgres/  # or snowflake2/
   dbt seed
   ```
2. Rebuild dependent models:
   ```bash
   dbt run --select +fact_trade_activity
   ```

## Success Criteria

Task 23 is successful when:

- ✅ Script executes without errors
- ✅ baseline/report.json created (10 models)
- ✅ candidate/report.json created (10 models)
- ✅ comparison_diff.json created
- ✅ All 10 models show PASS status
- ✅ Row counts match exactly
- ✅ Data hashes match exactly

## Next Steps

### If All Models Pass
✅ **Congratulations!** SQL translation is correct.
- All 10 marts produce identical outputs
- Proceed with deployment or next tasks

### If Some Models Fail
⚠️ **Proceed to Task #34 (Iteration and Fixes)**
1. Review failure details in comparison output
2. Identify root cause model (marts, intermediate, or staging)
3. Fix SQL in snowflake2/ folder
4. Re-run affected models: `dbt run --select model_name+`
5. Regenerate candidate report (repeat Step 2)
6. Re-run comparison (repeat Step 3)

## Implementation Checklist

- [x] Create run_comparison_task23.py automation script
- [x] Create run_task23.sh for Linux/macOS
- [x] Create run_task23.bat for Windows
- [x] Document prerequisites and setup
- [x] Document expected output formats
- [x] Document troubleshooting procedures
- [x] Document success criteria
- [x] Document next steps for both success and failure

## Technical Details

### Data Normalization

Both baseline and candidate reports use identical normalization:
- **Currency columns**: 2 decimal places
- **Rate columns**: 8 decimal places
- **Other numeric**: 8 decimal places
- **NULL values**: Empty strings
- **Row sorting**: Deterministic (all columns)
- **Hash algorithm**: SHA256

### Comparison Logic

The compare.py script:
1. Loads both reports
2. Validates structure
3. For each of 10 models:
   - Compares row_count (must be exact)
   - Compares data_hash (must be exact)
4. Generates results:
   - `passed`: Models with matching counts and hashes
   - `failed`: Models with mismatches
   - `missing`: Models in baseline but not candidate
   - `extra`: Models in candidate but not baseline
5. Outputs console display and JSON file

### Exit Codes

- `0`: All models passed (or comparison completed successfully)
- `1`: Some models failed or script error
- `130`: User interrupted (Ctrl+C)

## Related Documentation

- `BASELINE.md` - PostgreSQL baseline capture guide
- `TASK22_SUMMARY.md` - Snowflake candidate report generation
- `README.md` - Complete benchmark system documentation
- `compare.py` - Comparison script source code
- `generate-report.py` - Report generation script source code
