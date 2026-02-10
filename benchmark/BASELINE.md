# PostgreSQL Baseline Capture Guide

Complete guide for capturing baseline validation data from the PostgreSQL source database. This baseline serves as the ground truth for all Snowflake translation validation.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start](#quick-start)
4. [Detailed Instructions](#detailed-instructions)
5. [Verification](#verification)
6. [Troubleshooting](#troubleshooting)
7. [Success Criteria](#success-criteria)

## Overview

### Purpose

The baseline capture process extracts validation data from 10 marts models in the PostgreSQL database. This data includes:

- **Row counts**: Number of rows in each marts model
- **Data hashes**: SHA256 checksums of normalized, sorted data

This baseline becomes the reference point for validating that Snowflake translations produce identical results.

### What Gets Generated

Running the baseline capture creates:
- **File**: `baseline/report.json`
- **Contents**: JSON structure with metadata and metrics for 10 marts models
- **Size**: Typically 1-5 KB (contains hashes, not actual data)

### The 10 Marts Models

The following models are validated:

1. `fact_portfolio_summary`
2. `report_portfolio_overview`
3. `fact_portfolio_pnl`
4. `fact_trade_activity`
5. `report_daily_pnl`
6. `fact_fund_performance`
7. `fact_cashflow_waterfall`
8. `fact_portfolio_attribution`
9. `report_ic_dashboard`
10. `report_lp_quarterly`

## Prerequisites

### 1. PostgreSQL Database Running

The PostgreSQL database must be running and accessible at `localhost:5433`.

**Using Docker Compose** (recommended):
```bash
cd postgres/
docker-compose up -d
docker-compose ps  # Verify container is running
```

**Check connection**:
```bash
# Test with psql (if installed)
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d bain_analytics -c "SELECT 1"
```

### 2. Environment Variables Set

Three environment variables must be configured:

| Variable | Description | Example |
|----------|-------------|---------|
| `DBT_PG_USER` | PostgreSQL username | `postgres` |
| `DBT_PG_PASSWORD` | PostgreSQL password | `postgres` |
| `DBT_PG_DBNAME` | PostgreSQL database name | `bain_analytics` |

**Set variables** (Linux/macOS):
```bash
export DBT_PG_USER=postgres
export DBT_PG_PASSWORD=postgres
export DBT_PG_DBNAME=bain_analytics
```

**Set variables** (Windows):
```cmd
set DBT_PG_USER=postgres
set DBT_PG_PASSWORD=postgres
set DBT_PG_DBNAME=bain_analytics
```

### 3. dbt Models Built

All marts models must be materialized in the PostgreSQL database.

**Build models**:
```bash
cd postgres/
dbt run
```

**Verify models exist**:
```bash
# Count marts models in database
PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -d bain_analytics \
  -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE 'fact_%' OR table_name LIKE 'report_%';"
```

Expected result: Should show at least 10 tables/views.

### 4. Python Dependencies Installed

Required packages must be installed:

```bash
cd benchmark/
pip install -r requirements.txt
```

Or use the automated installer:
```bash
python install_and_verify.py
```

## Quick Start

### Option 1: Automated Python Script (Recommended)

```bash
cd benchmark/
python capture_baseline.py
```

This script:
- ✓ Checks environment variables
- ✓ Tests database connectivity
- ✓ Verifies dbt project structure
- ✓ Generates baseline/report.json
- ✓ Validates output

### Option 2: Bash Script (Linux/macOS)

```bash
cd benchmark/
chmod +x capture_baseline.sh
./capture_baseline.sh
```

### Option 3: Batch Script (Windows)

```cmd
cd benchmark
capture_baseline.bat
```

### Option 4: Manual Execution

```bash
cd benchmark/
python generate-report.py --dialect postgres --output baseline/report.json
```

## Detailed Instructions

### Step-by-Step Process

#### Step 1: Prepare Environment

1. **Navigate to benchmark directory**:
   ```bash
   cd benchmark/
   ```

2. **Set environment variables**:
   ```bash
   export DBT_PG_USER=postgres
   export DBT_PG_PASSWORD=postgres
   export DBT_PG_DBNAME=bain_analytics
   ```

3. **Verify database is running**:
   ```bash
   cd ../postgres/
   docker-compose ps
   cd ../benchmark/
   ```

#### Step 2: Build dbt Models

Ensure all marts models are materialized:

```bash
cd ../postgres/
dbt run --select marts
cd ../benchmark/
```

**Expected output**:
```
Running with dbt=1.x.x
Found 10 models...
Completed successfully
```

#### Step 3: Generate Baseline Report

Run the baseline capture script:

```bash
python capture_baseline.py
```

**Expected output**:
```
================================================================================
PostgreSQL Baseline Capture
================================================================================

[Step 1] Checking environment variables
--------------------------------------------------------------------------------
✓ All required environment variables are set

[Step 2] Checking PostgreSQL connection
--------------------------------------------------------------------------------
✓ PostgreSQL connection successful

[Step 3] Checking dbt project structure
--------------------------------------------------------------------------------
✓ All 10 required marts models found

[Step 4] Checking baseline directory
--------------------------------------------------------------------------------
✓ Baseline directory exists

[Step 5] Generating baseline report
--------------------------------------------------------------------------------
Running: python generate-report.py --dialect postgres --output baseline/report.json

2024-01-15 10:23:45,123 - INFO - Connecting to postgres...
2024-01-15 10:23:46,456 - INFO - Connected to Postgres at localhost:5433/bain_analytics
2024-01-15 10:23:46,789 - INFO - Processing 10 marts models...
2024-01-15 10:23:47,012 - INFO - Processing model: fact_portfolio_summary
2024-01-15 10:23:49,456 - INFO -   ✓ fact_portfolio_summary: 15247 rows, hash=a3d5f8c2...
2024-01-15 10:23:50,123 - INFO - Processing model: report_portfolio_overview
2024-01-15 10:23:52,789 - INFO -   ✓ report_portfolio_overview: 8934 rows, hash=b4e6g9d3...
...
2024-01-15 10:25:12,890 - INFO - Report saved to baseline/report.json

[Step 6] Verifying baseline output
--------------------------------------------------------------------------------
✓ Baseline report created successfully (Generated report size: 3245 bytes)

================================================================================
Baseline Capture Complete!
================================================================================
✓ baseline/report.json has been generated

Next steps:
  1. Run 'python verify_baseline.py' to validate the report structure
  2. Review baseline/report.json to ensure all models have data
  3. Use this baseline to compare against Snowflake candidate reports

================================================================================
```

#### Step 4: Verify Baseline Report

Validate that the generated report meets all success criteria:

```bash
python verify_baseline.py
```

**Expected output**:
```
================================================================================
Baseline Report Verification
================================================================================

Checking file existence
--------------------------------------------------------------------------------
✓ File found: ./baseline/report.json
  File size: 3245 bytes

Loading JSON
--------------------------------------------------------------------------------
✓ JSON loaded successfully

Validating report structure
--------------------------------------------------------------------------------
✓ Report structure is valid
  Generated at: 2024-01-15T10:25:12Z
  Dialect: postgres
  Database: bain_analytics
  Schema: public

Validating model count
--------------------------------------------------------------------------------
✓ Correct number of models: 10

Validating model entries
--------------------------------------------------------------------------------
✓ All model entries are valid

Model Summary
--------------------------------------------------------------------------------
✓ fact_portfolio_summary: 15247 rows, hash=a3d5f8c2e4b6d8f0...
✓ report_portfolio_overview: 8934 rows, hash=b4e6g9d3f5c7a1e9...
✓ fact_portfolio_pnl: 23456 rows, hash=c5d7a2f4e6b8d0f2...
✓ fact_trade_activity: 45123 rows, hash=d6e8b3f5c7a9e1d3...
✓ report_daily_pnl: 365 rows, hash=e7f9c4d6a8b0f2e4...
✓ fact_fund_performance: 1250 rows, hash=f8a0d5e7b9c1f3e5...
✓ fact_cashflow_waterfall: 567 rows, hash=a9b1e6f8c0d2f4e6...
✓ fact_portfolio_attribution: 2345 rows, hash=b0c2f7a9d1e3f5e7...
✓ report_ic_dashboard: 890 rows, hash=c1d3a8b0e2f4e6f8...
✓ report_lp_quarterly: 120 rows, hash=d2e4b9c1f3e5f7a9...

================================================================================
Validation Complete!
================================================================================
✓ baseline/report.json passes all validation checks

Success criteria met:
  ✓ Valid JSON structure
  ✓ Contains exactly 10 marts model entries
  ✓ All entries have required fields (model_name, row_count, data_hash)
  ✓ All row_count values are > 0
  ✓ All data_hash values are valid 64-character SHA256 hex strings

Next steps:
  1. Generate candidate report from Snowflake
  2. Compare baseline and candidate using compare.py

================================================================================
```

## Verification

### Inspect baseline/report.json

View the generated report:

```bash
cat baseline/report.json
```

**Expected structure**:
```json
{
  "generated_at": "2024-01-15T10:25:12Z",
  "dialect": "postgres",
  "database": "bain_analytics",
  "schema": "public",
  "models": {
    "fact_portfolio_summary": {
      "row_count": 15247,
      "data_hash": "a3d5f8c2e4b6d8f0a1c3e5d7f9b0c2e4d6f8a0c2e4b6d8f0a2c4e6d8f0a1c3e5"
    },
    "report_portfolio_overview": {
      "row_count": 8934,
      "data_hash": "b4e6g9d3f5c7a1e9d2f4e6b8d0f2e4c6a8b0d2f4e6b8d0f2e4c6a8b0d2f4e6b8"
    },
    ...
  }
}
```

### Manual Validation Checklist

- [ ] `baseline/report.json` file exists
- [ ] File size > 0 bytes (typically 1-5 KB)
- [ ] JSON is valid (can be parsed)
- [ ] Contains `generated_at`, `dialect`, `database`, `schema`, `models` fields
- [ ] `dialect` field equals `"postgres"`
- [ ] `models` object contains exactly 10 entries
- [ ] Each model entry has `row_count` and `data_hash` fields
- [ ] All `row_count` values are integers > 0
- [ ] All `data_hash` values are 64-character hex strings
- [ ] No model entries contain `error` fields

## Troubleshooting

### Error: "Missing required environment variables"

**Symptom**: Script fails immediately with missing variable error

**Solution**:
```bash
# Set all three required variables
export DBT_PG_USER=postgres
export DBT_PG_PASSWORD=postgres
export DBT_PG_DBNAME=bain_analytics

# Verify they are set
echo $DBT_PG_USER
echo $DBT_PG_PASSWORD
echo $DBT_PG_DBNAME
```

### Error: "Cannot connect to PostgreSQL"

**Symptom**: Connection refused or timeout errors

**Possible causes & solutions**:

1. **Database not running**:
   ```bash
   cd postgres/
   docker-compose up -d
   docker-compose ps  # Should show container as "Up"
   ```

2. **Wrong port or host**:
   - Verify PostgreSQL is accessible at `localhost:5433`
   - Check `postgres/docker-compose.yml` for port mapping

3. **Wrong credentials**:
   - Verify username/password match `docker-compose.yml`
   - Default: user=`postgres`, password=`postgres`

4. **Database doesn't exist**:
   ```bash
   # List databases
   PGPASSWORD=postgres psql -h localhost -p 5433 -U postgres -l
   ```

### Error: "Missing marts models"

**Symptom**: Script reports missing model SQL files

**Solution**:
```bash
# Verify marts directory exists
ls -la ../postgres/models/marts/

# Should list 10 .sql files:
# fact_portfolio_summary.sql
# report_portfolio_overview.sql
# fact_portfolio_pnl.sql
# fact_trade_activity.sql
# report_daily_pnl.sql
# fact_fund_performance.sql
# fact_cashflow_waterfall.sql
# fact_portfolio_attribution.sql
# report_ic_dashboard.sql
# report_lp_quarterly.sql
```

### Error: "Model has error" or "No rows returned"

**Symptom**: generate-report.py succeeds but models show errors or 0 rows

**Possible causes**:

1. **Models not built**:
   ```bash
   cd ../postgres/
   dbt run --select marts
   ```

2. **Empty seed data**:
   ```bash
   cd ../postgres/
   dbt seed
   dbt run
   ```

3. **Model query errors**:
   - Check dbt logs: `postgres/logs/dbt.log`
   - Review model SQL files for syntax errors

### Error: "Invalid JSON" or "File is empty"

**Symptom**: verify_baseline.py fails to parse report.json

**Solution**:
1. Check file contents:
   ```bash
   cat baseline/report.json
   ```

2. Look for error messages in the file

3. Re-run baseline capture:
   ```bash
   rm baseline/report.json
   python capture_baseline.py
   ```

### Row Count is Zero

**Symptom**: Model shows `row_count: 0` in report

**Possible causes**:

1. **No source data**: Check if seed files have data
2. **Model filters**: Model might have WHERE clauses that filter all rows
3. **Incremental model**: Might need full refresh

**Solutions**:
```bash
cd ../postgres/

# Reload seeds
dbt seed --full-refresh

# Rebuild models with full refresh
dbt run --select marts --full-refresh

# Return to benchmark
cd ../benchmark/
python capture_baseline.py
```

### Hash Validation Fails

**Symptom**: verify_baseline.py reports invalid SHA256 hash format

**Cause**: Bug in generate-report.py or corrupted JSON

**Solution**:
1. Delete baseline/report.json
2. Re-run capture_baseline.py
3. If problem persists, check generate-report.py for errors

## Success Criteria

### All Criteria Must Be Met

- [x] **baseline/report.json exists**: File is created in the baseline/ directory
- [x] **Valid JSON structure**: File can be parsed as valid JSON
- [x] **Exactly 10 model entries**: Models object contains all 10 marts models
- [x] **Required fields present**: Each model has `row_count` and `data_hash`
- [x] **Non-zero row counts**: All `row_count` values are > 0 (no empty models)
- [x] **Valid SHA256 hashes**: All `data_hash` values are 64-character hex strings
- [x] **No errors**: No model entries contain `error` fields
- [x] **Metadata complete**: Report has `generated_at`, `dialect`, `database`, `schema`

### Validation Commands

Run these commands to verify success criteria:

```bash
# Check file exists and has content
ls -lh baseline/report.json

# Validate JSON syntax
python -m json.tool baseline/report.json > /dev/null && echo "Valid JSON"

# Count models (should output: 10)
python -c "import json; print(len(json.load(open('baseline/report.json'))['models']))"

# Check all row counts > 0
python -c "import json; r=json.load(open('baseline/report.json')); print('All non-zero' if all(m['row_count']>0 for m in r['models'].values() if 'error' not in m) else 'Some zero')"

# Or use the automated verification script
python verify_baseline.py
```

## Next Steps

After successfully capturing the PostgreSQL baseline:

1. **Generate Snowflake candidate report**:
   ```bash
   python generate-report.py --dialect snowflake --output candidate/report.json
   ```

2. **Compare baseline vs candidate**:
   ```bash
   python compare.py --baseline baseline/report.json --candidate candidate/report.json
   ```

3. **Review comparison results**: Identify any data discrepancies between PostgreSQL and Snowflake

4. **Iterate on translation**: Fix any Snowflake SQL translation issues and re-run comparison

## Additional Resources

- **Main README**: `benchmark/README.md` - Complete benchmark system documentation
- **Installation Guide**: `benchmark/INSTALL.md` - Dependency installation instructions
- **generate-report.py**: Core script that extracts and hashes data
- **verify_baseline.py**: Automated validation of baseline reports
- **compare.py**: Comparison tool for baseline vs candidate reports

## Summary

The baseline capture process:
1. ✓ Validates environment and prerequisites
2. ✓ Extracts row counts and data hashes from 10 PostgreSQL marts models
3. ✓ Generates `baseline/report.json` with validation metrics
4. ✓ Verifies output meets all success criteria

This baseline serves as the ground truth for all subsequent Snowflake translation validation comparisons.
