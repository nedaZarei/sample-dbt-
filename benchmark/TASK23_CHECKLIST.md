# Task 23 Execution Checklist

Use this checklist to verify that Task 23 (Compare Baseline vs Candidate Outputs) has been executed successfully.

---

## Pre-Execution Checklist

### Environment Setup

- [ ] **PostgreSQL Database Running**
  ```bash
  cd postgres/
  docker-compose ps  # Should show container running
  ```

- [ ] **Snowflake Account Accessible**
  - Account identifier: PZXTMSC-WP48482 (or your account)
  - Credentials available

- [ ] **PostgreSQL Environment Variables Set**
  - [ ] `DBT_PG_USER` (e.g., postgres)
  - [ ] `DBT_PG_PASSWORD`
  - [ ] `DBT_PG_DBNAME` (e.g., bain_analytics)

- [ ] **Snowflake Environment Variables Set**
  - [ ] `SNOWFLAKE_ACCOUNT`
  - [ ] `SNOWFLAKE_USER`
  - [ ] `SNOWFLAKE_PASSWORD`

- [ ] **Python Dependencies Installed**
  ```bash
  cd benchmark/
  pip install -r requirements.txt
  ```

- [ ] **PostgreSQL Models Built**
  ```bash
  cd postgres/
  dbt seed
  dbt run --select marts
  ```

- [ ] **Snowflake Models Built**
  ```bash
  cd snowflake2/
  dbt seed
  dbt run --select marts
  ```

---

## Execution Checklist

### Step 1: Navigate to Benchmark Directory

- [ ] Changed to benchmark directory
  ```bash
  cd benchmark/
  ```

### Step 2: Run Comparison Script

Choose one method:

**Option A: Shell Script (Linux/macOS)**
- [ ] Made script executable: `chmod +x run_task23.sh`
- [ ] Executed script: `./run_task23.sh`

**Option B: Batch Script (Windows)**
- [ ] Executed script: `run_task23.bat`

**Option C: Python Script (All Platforms)**
- [ ] Executed script: `python3 run_comparison_task23.py`

### Step 3: Monitor Execution

- [ ] Script started successfully (no immediate errors)
- [ ] Environment variables validated (✓ All required environment variables are set)
- [ ] Baseline report generation started
- [ ] Baseline report generated (✓ Baseline report generated)
- [ ] Baseline report verified (✓ Baseline report verified)
- [ ] Candidate report generation started
- [ ] Candidate report generated (✓ Candidate report generated)
- [ ] Candidate report verified (✓ Candidate report verified)
- [ ] Comparison executed
- [ ] Summary generated

---

## Post-Execution Checklist

### Verify Output Files Created

- [ ] **baseline/report.json exists**
  ```bash
  ls -lh baseline/report.json
  # Should show ~3 KB file
  ```

- [ ] **candidate/report.json exists**
  ```bash
  ls -lh candidate/report.json
  # Should show ~3 KB file
  ```

- [ ] **comparison_diff.json exists**
  ```bash
  ls -lh comparison_diff.json
  # Should show ~5 KB file
  ```

### Verify File Contents

- [ ] **baseline/report.json is valid JSON**
  ```bash
  python3 -m json.tool baseline/report.json > /dev/null && echo "Valid JSON"
  ```

- [ ] **candidate/report.json is valid JSON**
  ```bash
  python3 -m json.tool candidate/report.json > /dev/null && echo "Valid JSON"
  ```

- [ ] **comparison_diff.json is valid JSON**
  ```bash
  python3 -m json.tool comparison_diff.json > /dev/null && echo "Valid JSON"
  ```

### Verify Model Coverage

- [ ] **Baseline report contains 10 models**
  ```bash
  python3 -c "import json; r=json.load(open('baseline/report.json')); print(f'Models: {len(r[\"models\"])}')"
  # Should print: Models: 10
  ```

- [ ] **Candidate report contains 10 models**
  ```bash
  python3 -c "import json; r=json.load(open('candidate/report.json')); print(f'Models: {len(r[\"models\"])}')"
  # Should print: Models: 10
  ```

---

## Results Verification Checklist

### Review Comparison Output

- [ ] **Comparison summary displayed**
  - Check console output for summary section

- [ ] **All 10 models listed**
  - [ ] fact_portfolio_summary
  - [ ] report_portfolio_overview
  - [ ] fact_portfolio_pnl
  - [ ] fact_trade_activity
  - [ ] report_daily_pnl
  - [ ] fact_fund_performance
  - [ ] fact_cashflow_waterfall
  - [ ] fact_portfolio_attribution
  - [ ] report_ic_dashboard
  - [ ] report_lp_quarterly

### Check Model Status

For each model, verify status:

- [ ] **fact_portfolio_summary**: ✓ PASS / ✗ FAIL / ⚠ MISSING
- [ ] **report_portfolio_overview**: ✓ PASS / ✗ FAIL / ⚠ MISSING
- [ ] **fact_portfolio_pnl**: ✓ PASS / ✗ FAIL / ⚠ MISSING
- [ ] **fact_trade_activity**: ✓ PASS / ✗ FAIL / ⚠ MISSING
- [ ] **report_daily_pnl**: ✓ PASS / ✗ FAIL / ⚠ MISSING
- [ ] **fact_fund_performance**: ✓ PASS / ✗ FAIL / ⚠ MISSING
- [ ] **fact_cashflow_waterfall**: ✓ PASS / ✗ FAIL / ⚠ MISSING
- [ ] **fact_portfolio_attribution**: ✓ PASS / ✗ FAIL / ⚠ MISSING
- [ ] **report_ic_dashboard**: ✓ PASS / ✗ FAIL / ⚠ MISSING
- [ ] **report_lp_quarterly**: ✓ PASS / ✗ FAIL / ⚠ MISSING

### Verify Summary Statistics

- [ ] **Total models**: 10
- [ ] **Passed count**: ______ (fill in)
- [ ] **Failed count**: ______ (fill in)
- [ ] **Missing count**: ______ (fill in)
- [ ] **Extra count**: ______ (fill in)

---

## Success Criteria Verification

### All Checks Must Pass

- [ ] ✅ Script executed without errors (exit code 0)
- [ ] ✅ baseline/report.json created (size > 100 bytes)
- [ ] ✅ candidate/report.json created (size > 100 bytes)
- [ ] ✅ comparison_diff.json created (size > 100 bytes)
- [ ] ✅ All 3 files are valid JSON
- [ ] ✅ Both reports contain exactly 10 models
- [ ] ✅ All 10 models have non-zero row counts
- [ ] ✅ Comparison executed successfully

### Success Outcome

**Complete Success:**
- [ ] ✅ **All 10 models show PASS status**
- [ ] ✅ **No failed models**
- [ ] ✅ **No missing models**
- [ ] ✅ **Row counts match exactly**
- [ ] ✅ **Data hashes match exactly**

**Partial Success (Some Failures):**
- [ ] ⚠️ **Some models show FAIL status**
- [ ] ⚠️ **Failures documented below**

---

## Failure Documentation (If Applicable)

### Failed Models

If any models failed, document them here:

| Model Name | Issue Type | Details |
|------------|------------|---------|
| (example) fact_trade_activity | row_count mismatch | baseline: 45123, candidate: 45120 |
| | | |
| | | |
| | | |

### Issue Types

- **row_count mismatch**: Different number of rows
- **data_hash mismatch**: Same row count, different data content
- **missing**: Model in baseline but not in candidate
- **extra**: Model in candidate but not in baseline

---

## Next Steps

### If All Models Pass ✅

**Task 23 Complete!**

- [ ] **Document Success**
  - All 10 marts validated successfully
  - PostgreSQL and Snowflake outputs are identical

- [ ] **Archive Results**
  - [ ] Save comparison_diff.json
  - [ ] Save baseline/report.json
  - [ ] Save candidate/report.json

- [ ] **Proceed to Next Task**
  - Task 23 complete
  - Ready for deployment or next workflow step

### If Some Models Fail ❌

**Proceed to Task #34: Fix Validation Failures**

- [ ] **Review Failures**
  - [ ] Note which models failed
  - [ ] Identify issue types (row count vs hash mismatch)
  - [ ] Review console output for details

- [ ] **Identify Root Cause**
  - [ ] For row count mismatches: Check WHERE/JOIN logic
  - [ ] For hash mismatches: Check calculations and data types

- [ ] **Fix Snowflake SQL**
  - [ ] Edit model(s) in snowflake2/models/marts/
  - [ ] Apply necessary corrections

- [ ] **Re-run Affected Models**
  ```bash
  cd snowflake2/
  dbt run --select failed_model_name+
  ```

- [ ] **Regenerate Candidate Report**
  ```bash
  cd benchmark/
  python3 generate-report.py --dialect snowflake --output candidate/report.json
  ```

- [ ] **Re-run Comparison**
  ```bash
  python3 compare.py \
    --baseline baseline/report.json \
    --candidate candidate/report.json \
    --output comparison_diff.json
  ```

- [ ] **Repeat Until All Pass**

---

## Troubleshooting Checklist

### Issue: Missing Environment Variables

- [ ] Verified all 6 variables are set
- [ ] Re-exported variables
- [ ] Tried alternative method (inline or .env file)

### Issue: PostgreSQL Connection Failed

- [ ] Verified PostgreSQL container is running
- [ ] Checked port 5433 is accessible
- [ ] Verified credentials are correct
- [ ] Tried connecting with psql client

### Issue: Snowflake Connection Failed

- [ ] Verified account identifier format
- [ ] Checked credentials
- [ ] Verified network connectivity
- [ ] Tried SnowSQL connection test

### Issue: Model Not Found

- [ ] Ran `dbt run --select model_name` in appropriate folder
- [ ] Verified model exists in database
- [ ] Checked dbt logs for build errors

### Issue: Zero Row Count

- [ ] Ran `dbt seed` to load seed data
- [ ] Ran `dbt run` to build models
- [ ] Verified seed files exist
- [ ] Checked upstream dependencies

---

## Sign-Off

### Execution Details

- **Executed by**: ________________
- **Date**: ________________
- **Time**: ________________
- **Platform**: Linux / macOS / Windows (circle one)

### Results

- **Total Models**: 10
- **Passed**: ______
- **Failed**: ______
- **Success**: YES / NO (circle one)

### Notes

```
(Add any additional notes or observations here)
```

---

## Documentation Reference

For detailed information, refer to:

- **Quick Start**: `TASK23_QUICKSTART.md`
- **Comprehensive Guide**: `TASK23_COMPARISON.md`
- **Architecture**: `TASK23_README.md`
- **Summary**: `TASK23_EXECUTION_SUMMARY.md`
- **Troubleshooting**: See TASK23_COMPARISON.md section

---

## Checklist Complete

- [ ] ✅ **All pre-execution items verified**
- [ ] ✅ **Script executed successfully**
- [ ] ✅ **Output files verified**
- [ ] ✅ **Results reviewed**
- [ ] ✅ **Success criteria met OR failures documented**
- [ ] ✅ **Next steps identified**

**Task 23 Status**: COMPLETE / IN PROGRESS / REQUIRES ITERATION (circle one)
