# Benchmark Validation System Documentation

Complete documentation for the benchmark validation system that compares data quality metrics across Postgres and Snowflake databases.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Environment Variables](#environment-variables)
5. [Usage Examples](#usage-examples)
6. [Understanding Results](#understanding-results)
7. [Numeric Rounding Strategy](#numeric-rounding-strategy)
8. [Troubleshooting](#troubleshooting)
9. [CI/CD Integration](#cicd-integration)
10. [Technical Details](#technical-details)
11. [Warnings and Gotchas](#warnings-and-gotchas)

## Overview

### Purpose

The benchmark validation system provides a comprehensive approach to validating data consistency across different database platforms during migration or data pipeline verification. It compares **10 MARTS (fact/report) models** between a baseline database (Postgres) and a candidate database (Snowflake), identifying differences in:

- **Row counts**: Exact match required
- **Data content**: Validated through SHA256 hash checksums
- **Numeric precision**: Handled intelligently for currency and rate columns

### Approach

The validation workflow consists of three steps:

1. **Generate Baseline Report** - Extract and hash data from Postgres MARTS models
2. **Generate Candidate Report** - Extract and hash data from Snowflake MARTS models
3. **Compare Reports** - Identify differences and produce both human-readable and JSON outputs

### Scope: 10 MARTS Models

The system validates the following 10 models:

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

These models are hardcoded in both `generate-report.py` and `compare.py` and must remain synchronized.

## Prerequisites

### System Requirements

- **Python**: 3.7 or higher
- **pip**: For package installation
- **Network Access**: To both Postgres and Snowflake databases

### Database Access

#### Postgres Requirements

- Access to a Postgres instance at `localhost:5433`
- Read permissions on all 10 MARTS models in the target schema
- Account with the following environment variables:
  - `DBT_PG_USER`: Postgres username
  - `DBT_PG_PASSWORD`: Postgres password
  - `DBT_PG_DBNAME`: Postgres database name

#### Snowflake Requirements

- Active Snowflake account
- Read permissions on all 10 MARTS models
- Account with the following environment variables:
  - `SNOWFLAKE_ACCOUNT`: Snowflake account identifier
  - `SNOWFLAKE_USER`: Snowflake username
  - `SNOWFLAKE_PASSWORD`: Snowflake password
- Warehouse: `COMPUTE_WH` (default, configurable in code)
- Database: `DBT_DEMO` (default, configurable in code)
- Schema: `DEV` (default, configurable in code)

### dbt Requirements

Before running the validation scripts:

1. All 10 MARTS models must exist in both databases
2. Models should be in the same schema in each database
3. Run `dbt run` to build models before generating reports
4. Ensure models are materialized (as tables or views compatible with `SELECT *`)

## Installation

### Step 1: Clone or Navigate to the Project

```bash
cd benchmark/
```

### Step 2: Create a Virtual Environment (Recommended)

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

The `requirements.txt` includes:
- `psycopg2-binary`: Postgres database driver
- `snowflake-connector-python`: Snowflake database driver

### Step 4: Optional - Install colorama for Colored Output

```bash
pip install colorama
```

The system gracefully handles the absence of colorama by outputting plain text instead. Colorama is **optional** and falls back gracefully.

### Step 5: Verify Installation

```bash
python3 generate-report.py --help
python3 compare.py --help
```

Both should display their help messages without errors.

## Environment Variables

### Complete List with Examples

#### Postgres Variables

| Variable | Description | Example | Required |
|----------|-------------|---------|----------|
| `DBT_PG_USER` | Postgres username | `postgres` | Yes |
| `DBT_PG_PASSWORD` | Postgres password | `my_secure_password` | Yes |
| `DBT_PG_DBNAME` | Postgres database name | `bain_analytics` | Yes |

#### Snowflake Variables

| Variable | Description | Example | Required |
|----------|-------------|---------|----------|
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier | `xy12345.us-east-1` | Yes |
| `SNOWFLAKE_USER` | Snowflake username | `analyst@company.com` | Yes |
| `SNOWFLAKE_PASSWORD` | Snowflake password | `snowflake_password_123` | Yes |

### .env File Template

Create a `.env` file in the `benchmark/` directory with the following template. **Never commit this file to version control.**

```bash
# Postgres Configuration
DBT_PG_USER=postgres
DBT_PG_PASSWORD=your_postgres_password
DBT_PG_DBNAME=bain_analytics

# Snowflake Configuration
SNOWFLAKE_ACCOUNT=xy12345.us-east-1
SNOWFLAKE_USER=analyst@company.com
SNOWFLAKE_PASSWORD=your_snowflake_password
```

### How to Load Environment Variables

#### Option 1: Using a .env file with python-dotenv

```bash
pip install python-dotenv
```

Then create a small wrapper:

```python
from dotenv import load_dotenv
load_dotenv()
```

#### Option 2: Export directly in shell

```bash
export DBT_PG_USER=postgres
export DBT_PG_PASSWORD=my_password
export DBT_PG_DBNAME=bain_analytics
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=analyst@company.com
export SNOWFLAKE_PASSWORD=snowflake_password
```

#### Option 3: Inline for single execution

```bash
DBT_PG_USER=postgres DBT_PG_PASSWORD=pwd DBT_PG_DBNAME=db \
  SNOWFLAKE_ACCOUNT=acc SNOWFLAKE_USER=user SNOWFLAKE_PASSWORD=pwd \
  python3 generate-report.py --dialect postgres --output baseline.json
```

## Usage Examples

### Complete Workflow with Step-by-Step Commands

#### Step 1: Generate Baseline Report from Postgres

Generate a report containing row counts and hashes for all 10 MARTS models from your baseline Postgres database.

```bash
# Set environment variables first
export DBT_PG_USER=postgres
export DBT_PG_PASSWORD=mypassword
export DBT_PG_DBNAME=bain_analytics

# Run dbt to ensure models are built
dbt run

# Generate baseline report
python3 generate-report.py \
  --dialect postgres \
  --output baseline/report.json
```

**Expected output:**
```
2024-01-15 10:23:45,123 - INFO - Connecting to postgres...
2024-01-15 10:23:46,456 - INFO - Connected to Postgres at localhost:5433/bain_analytics
2024-01-15 10:23:46,789 - INFO - Processing 10 marts models...
2024-01-15 10:23:47,012 - INFO - Processing model: fact_portfolio_summary
2024-01-15 10:23:49,456 - INFO -   ✓ fact_portfolio_summary: 15247 rows, hash=a3d5f8c2...
2024-01-15 10:23:50,123 - INFO - Processing model: report_portfolio_overview
...
2024-01-15 10:25:12,890 - INFO - Report saved to baseline/report.json
```

**Generates file:** `baseline/report.json`

#### Step 2: Generate Candidate Report from Snowflake

Generate a report containing row counts and hashes for all 10 MARTS models from your candidate Snowflake database.

```bash
# Set Snowflake environment variables
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
export SNOWFLAKE_USER=analyst@company.com
export SNOWFLAKE_PASSWORD=mysnowflakepass

# Generate candidate report
python3 generate-report.py \
  --dialect snowflake \
  --output candidate/report.json
```

**Expected output:**
```
2024-01-15 10:30:00,111 - INFO - Connecting to snowflake...
2024-01-15 10:30:02,222 - INFO - Connected to Snowflake account xy12345.us-east-1, database DBT_DEMO, schema DEV
2024-01-15 10:30:02,333 - INFO - Processing 10 marts models...
2024-01-15 10:30:05,444 - INFO - Processing model: fact_portfolio_summary
2024-01-15 10:30:08,555 - INFO -   ✓ fact_portfolio_summary: 15247 rows, hash=a3d5f8c2...
...
2024-01-15 10:32:30,666 - INFO - Report saved to candidate/report.json
```

**Generates file:** `candidate/report.json`

#### Step 3: Compare Reports

Compare the baseline and candidate reports to identify differences.

```bash
python3 compare.py \
  --baseline baseline/report.json \
  --candidate candidate/report.json \
  --output comparison_diff.json
```

**Expected output (all passing):**
```
================================================================================
Validation Report Comparison
================================================================================

Baseline: postgres (bain_analytics)
  Generated: 2024-01-15T10:25:12Z

Candidate: snowflake (DBT_DEMO)
  Generated: 2024-01-15T10:32:30Z

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
```

**Exit code:** `0` (success)

**Generates file:** `comparison_diff.json`

### Example Output with Failures

If differences are detected:

```
================================================================================
Validation Report Comparison
================================================================================

Baseline: postgres (bain_analytics)
  Generated: 2024-01-15T10:25:12Z

Candidate: snowflake (DBT_DEMO)
  Generated: 2024-01-15T10:32:30Z

--------------------------------------------------------------------------------
Results:
--------------------------------------------------------------------------------

✓ PASS: fact_portfolio_summary (15247 rows, hash match)
✓ PASS: report_portfolio_overview (8934 rows, hash match)
✗ FAIL: fact_portfolio_pnl
        row_count mismatch: 23456 vs 23455, data_hash mismatch
✓ PASS: fact_trade_activity (45123 rows, hash match)
✓ PASS: report_daily_pnl (365 rows, hash match)
✗ FAIL: fact_fund_performance
        data_hash mismatch
⚠ MISSING: fact_cashflow_waterfall (not found in candidate)
✓ PASS: fact_portfolio_attribution (2345 rows, hash match)
✓ PASS: report_ic_dashboard (890 rows, hash match)
✓ PASS: report_lp_quarterly (120 rows, hash match)

--------------------------------------------------------------------------------
Summary:
  Total models: 10
  Passed: 7
  Failed: 2
  Missing: 1
  Extra: 0
================================================================================
```

**Exit code:** `1` (failure)

## Understanding Results

### Result Categories

#### ✓ PASS
- **Meaning**: Model matches perfectly between baseline and candidate
- **Requirements met**:
  - Row counts are identical
  - Data hashes match (content is identical after normalization)
  - No errors in either report
- **Action required**: None - validation successful for this model

#### ✗ FAIL
- **Meaning**: Model has discrepancies between baseline and candidate
- **Possible issues**:
  - **row_count mismatch**: Different number of rows (e.g., `15247 vs 15246`)
  - **data_hash mismatch**: Same row count but different data content
  - **Both**: Row count and content differ
- **Action required**: Investigate the source of data differences

#### ⚠ MISSING
- **Meaning**: Model exists in baseline but not in candidate
- **Possible causes**:
  - Candidate report generation failed for this model
  - Model not yet built in Snowflake
  - Different schema or naming in Snowflake
- **Action required**: Verify model exists in Snowflake and report was generated correctly

#### ⚠ EXTRA
- **Meaning**: Model exists in candidate but not in baseline
- **Possible causes**:
  - Model doesn't exist in Postgres
  - Different schema or naming in Postgres
- **Action required**: Verify model consistency across environments

### Examples of Different Scenarios

**Scenario 1: Perfect Match (All PASS)**
- All 10 models match row count and hash
- No data synchronization issues
- Safe to proceed with full migration

**Scenario 2: Minor Row Count Difference (FAIL)**
- Model A: 1,000,000 rows in Postgres, 999,999 rows in Snowflake
- Likely cause: Timing difference in incremental loads or soft-delete handling
- Investigation: Check if deletion logic differs between databases

**Scenario 3: Hash Mismatch with Same Row Count (FAIL)**
- Model B: 500,000 rows in both, but different hash
- Likely cause: Floating-point rounding differences, NULL handling, or data type conversions
- Investigation: Review data normalization rules (see Numeric Rounding Strategy section)

## Numeric Rounding Strategy

### Why Rounding Matters

Database systems handle numeric precision differently:
- **Postgres** might store `0.123456789` exactly as a DECIMAL
- **Snowflake** might round to different precision
- Without normalization, matching databases with identical business data would fail

### Rounding Rules

The system applies intelligent rounding based on **column name patterns**:

#### Currency Columns: 2 Decimals

Keywords triggering 2-decimal rounding: `amount`, `value`, `mv`, `pnl`, `commission`

Examples:
- Column `transaction_amount`: rounded to 2 decimals → `1234.56`
- Column `market_value` (mv): rounded to 2 decimals → `9999.99`
- Column `commission_pnl`: rounded to 2 decimals → `50.25`

**Rationale**: Currency values are typically tracked to the cent level

#### Rate Columns: 8 Decimals

Keywords triggering 8-decimal rounding: `rate`, `return`, `pct`, `percentage`

Examples:
- Column `interest_rate`: rounded to 8 decimals → `0.045678901`
- Column `annual_return`: rounded to 8 decimals → `0.124567890`
- Column `fee_percentage`: rounded to 8 decimals → `0.005000000`

**Rationale**: Rate columns need higher precision for accurate calculations

#### All Other Numeric Columns: 8 Decimals

Any numeric column not matching currency or rate keywords defaults to 8 decimals.

Examples:
- Column `transaction_count`: stored as integer, no rounding needed
- Column `adjusted_factor`: rounded to 8 decimals

### NULL Value Handling

- NULL values are represented as empty strings during hashing
- Both databases must handle NULLs the same way (trailing NULLs, COALESCE, etc.)
- Mismatch in NULL counts will cause hash differences

### Sorting for Deterministic Hashing

Rows are sorted before hashing to ensure consistent hash values regardless of:
- Record insertion order
- Query execution plan differences
- Database internal ordering

**Sort key**: All columns treated as strings, with NULL values treated as empty strings

## Troubleshooting

### Connection Errors

#### Error: "Missing required Postgres environment variables"

**Cause**: One or more Postgres environment variables not set

**Solution**:
```bash
# Verify all three variables are set
echo $DBT_PG_USER
echo $DBT_PG_PASSWORD
echo $DBT_PG_DBNAME

# Export them if missing
export DBT_PG_USER=postgres
export DBT_PG_PASSWORD=yourpassword
export DBT_PG_DBNAME=yourdatabase
```

#### Error: "Failed to connect to Postgres: connection refused"

**Cause**: Postgres is not running or not at the expected host/port

**Solution**:
```bash
# Check if Postgres is running
psql -h localhost -p 5433 -U postgres -c "SELECT 1"

# If port is different, edit generate-report.py line 81:
# Change: self.port = 5433
# To: self.port = 5432 (or your port)
```

#### Error: "Failed to connect to Snowflake: invalid account identifier"

**Cause**: `SNOWFLAKE_ACCOUNT` is not in the correct format

**Solution**:
```bash
# Correct format: account_identifier or account_identifier.region
# Example: xy12345.us-east-1 or xy12345

export SNOWFLAKE_ACCOUNT=xy12345.us-east-1
```

### Missing Models and dbt Failures

#### Error: "Model 'fact_portfolio_summary' does not exist"

**Cause**: Model not yet built in the database, or in wrong schema

**Solution**:
```bash
# Ensure dbt run has been executed
dbt run

# Verify model exists
# Postgres:
psql -h localhost -p 5433 -U postgres -d yourdatabase \
  -c "SELECT COUNT(*) FROM fact_portfolio_summary;"

# Snowflake:
snowsql -a xy12345.us-east-1 -u user \
  -d DBT_DEMO -s DEV \
  -q "SELECT COUNT(*) FROM fact_portfolio_summary;"
```

#### Error: "Query timeout after 300 seconds"

**Cause**: Large model takes longer than 5 minutes to query

**Solution**:
```python
# Edit generate-report.py line 456 to increase timeout:
rows, column_names = self.connector.query(sql, timeout=600)  # 10 minutes
```

### Hash Mismatches

#### "data_hash mismatch" but same row count

**Cause**: Subtle data differences due to rounding, NULL handling, or type conversion

**Diagnostic steps**:
```bash
# 1. Check a sample of data in both databases
# Postgres:
SELECT * FROM fact_portfolio_summary LIMIT 10;

# Snowflake:
SELECT * FROM FACT_PORTFOLIO_SUMMARY LIMIT 10;

# 2. Look for:
# - Different NULL representations
# - Floating-point precision differences (e.g., 0.333333 vs 0.33333333)
# - Case differences (unlikely but possible with case-sensitive columns)
# - Whitespace differences in string columns
```

**Solution options**:
- Review the numeric rounding rules (section above)
- Check if additional data preprocessing is needed
- Verify column names match between databases (case-sensitivity)

#### "row_count mismatch"

**Cause**: Different numbers of rows between databases

**Solution**:
```bash
# Get exact counts from both databases
# Postgres
psql -c "SELECT COUNT(*) FROM fact_portfolio_summary;" -h localhost -p 5433 -U postgres

# Snowflake
snowsql -q "SELECT COUNT(*) FROM fact_portfolio_summary;"

# Find missing/extra rows
# In Postgres, find rows not in Snowflake (requires primary key/unique identifier):
SELECT * FROM fact_portfolio_summary_postgres
EXCEPT
SELECT * FROM fact_portfolio_summary_snowflake;
```

### Floating-Point Edge Cases

#### Scenario: Same logical number, different representations

**Problem**: 
- Postgres: `0.1 + 0.2 = 0.30000000000000004`
- Snowflake: `0.1 + 0.2 = 0.3`

**Why rounding helps**:
- Round to 8 decimals: both become `0.3` or `0.30000000`
- Hash matches despite underlying difference

**If still failing**:
- Use CAST/ROUND in dbt models to ensure consistency before comparison
- Example:
```sql
SELECT
  ROUND(interest_rate::NUMERIC, 8) as interest_rate
FROM fact_rates
```

### Performance Issues with Large Datasets

#### Issue: Memory usage grows very large

**Cause**: Large models loaded entirely into memory for hashing

**Symptoms**:
- Process uses multiple GB of RAM
- Process becomes slow during normalization

**Solutions**:

Option 1: Increase available memory
```bash
# Monitor memory during generation
watch free -h

# If system memory insufficient, upgrade instance
```

Option 2: Optimize the query (edit generate-report.py)
```python
# Sample data if model is very large
sql = f"SELECT * FROM {model_name} TABLESAMPLE BERNOULLI (10)"  # 10% sample
```

#### Issue: Network timeout during query execution

**Symptoms**:
- "Connection timeout" or "Operation timed out" after several minutes
- Works with small models, fails with large ones

**Solutions**:
```bash
# 1. Increase query timeout
# Edit generate-report.py line 456:
rows, column_names = self.connector.query(sql, timeout=900)  # 15 minutes

# 2. Run during off-peak hours with fewer competing queries

# 3. Simplify the query by excluding columns if acceptable
sql = f"SELECT key_cols, numeric_cols FROM {model_name}"
```

## CI/CD Integration

### Exit Codes

The scripts use standard Unix exit codes for CI/CD integration:

| Exit Code | Meaning | Use Case |
|-----------|---------|----------|
| `0` | Success - all models passed validation | Proceed with deployment |
| `1` | Failure - one or more models failed/missing | Block deployment, investigate |
| `130` | Cancellation - user pressed Ctrl+C | Treat as failure in CI/CD |

### GitHub Actions Example

```yaml
name: Benchmark Validation

on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM UTC
  workflow_dispatch:      # Manual trigger

jobs:
  validation:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      
      - name: Install dependencies
        run: |
          cd benchmark
          pip install -r requirements.txt
      
      - name: Generate baseline report (Postgres)
        env:
          DBT_PG_USER: ${{ secrets.DBT_PG_USER }}
          DBT_PG_PASSWORD: ${{ secrets.DBT_PG_PASSWORD }}
          DBT_PG_DBNAME: ${{ secrets.DBT_PG_DBNAME }}
        run: |
          cd benchmark
          python3 generate-report.py --dialect postgres --output baseline.json
      
      - name: Generate candidate report (Snowflake)
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
        run: |
          cd benchmark
          python3 generate-report.py --dialect snowflake --output candidate.json
      
      - name: Compare reports
        run: |
          cd benchmark
          python3 compare.py \
            --baseline baseline.json \
            --candidate candidate.json \
            --output comparison.json
      
      - name: Upload comparison results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-comparison
          path: benchmark/comparison.json
      
      - name: Comment PR with results
        if: always()
        uses: actions/github-script@v6
        with:
          script: |
            const fs = require('fs');
            const comparison = JSON.parse(fs.readFileSync('benchmark/comparison.json', 'utf8'));
            const summary = comparison.summary;
            
            const comment = `## Benchmark Validation Results
            - ✓ Passed: ${summary.passed}
            - ✗ Failed: ${summary.failed}
            - ⚠ Missing: ${summary.missing}
            - ⚠ Extra: ${summary.extra}
            `;
            
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: comment
            });
```

### Jenkins Pipeline Example

```groovy
pipeline {
    agent any
    
    triggers {
        cron('0 2 * * *')  // Daily at 2 AM
    }
    
    environment {
        DBT_PG_USER = credentials('postgres-user')
        DBT_PG_PASSWORD = credentials('postgres-password')
        DBT_PG_DBNAME = credentials('postgres-dbname')
        SNOWFLAKE_ACCOUNT = credentials('snowflake-account')
        SNOWFLAKE_USER = credentials('snowflake-user')
        SNOWFLAKE_PASSWORD = credentials('snowflake-password')
    }
    
    stages {
        stage('Setup') {
            steps {
                sh '''
                    cd benchmark
                    python3 -m venv venv
                    . venv/bin/activate
                    pip install -r requirements.txt
                '''
            }
        }
        
        stage('Generate Baseline') {
            steps {
                sh '''
                    cd benchmark
                    . venv/bin/activate
                    python3 generate-report.py \
                        --dialect postgres \
                        --output baseline.json
                '''
            }
        }
        
        stage('Generate Candidate') {
            steps {
                sh '''
                    cd benchmark
                    . venv/bin/activate
                    python3 generate-report.py \
                        --dialect snowflake \
                        --output candidate.json
                '''
            }
        }
        
        stage('Compare') {
            steps {
                sh '''
                    cd benchmark
                    . venv/bin/activate
                    python3 compare.py \
                        --baseline baseline.json \
                        --candidate candidate.json \
                        --output comparison.json
                '''
            }
        }
        
        stage('Check Results') {
            steps {
                script {
                    def comparison = readJSON file: 'benchmark/comparison.json'
                    def failed = comparison.summary.failed
                    def missing = comparison.summary.missing
                    
                    if (failed > 0 || missing > 0) {
                        error("Validation failed: ${failed} failures, ${missing} missing")
                    }
                }
            }
        }
    }
    
    post {
        always {
            archiveArtifacts artifacts: 'benchmark/comparison.json'
            cleanWs()
        }
    }
}
```

### Handling Exit Codes in Scripts

```bash
#!/bin/bash
set -e  # Exit on any error

cd benchmark

# Generate reports
python3 generate-report.py --dialect postgres --output baseline.json
python3 generate-report.py --dialect snowflake --output candidate.json

# Compare
python3 compare.py \
  --baseline baseline.json \
  --candidate candidate.json \
  --output comparison.json

# Capture exit code
EXIT_CODE=$?

# Log results
if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Validation PASSED"
    exit 0
elif [ $EXIT_CODE -eq 1 ]; then
    echo "✗ Validation FAILED"
    exit 1
elif [ $EXIT_CODE -eq 130 ]; then
    echo "⚠ Validation CANCELLED"
    exit 1  # Treat as failure in CI/CD
fi
```

## Technical Details

### Hash Algorithm

#### SHA256 Algorithm

The system uses **SHA256** (Secure Hash Algorithm 256-bit) for data validation:

- **Algorithm**: SHA256 (SHA-2 family)
- **Output**: 64-character hexadecimal string
- **Collision resistance**: Cryptographically secure (extremely unlikely to have two different datasets with same hash)
- **Implementation**: Python's `hashlib.sha256()`

Example:
```
Data: "ABC|123\n"
SHA256 Hash: e1d6c2f0d1e8b3c5a9f7e2c8d4b6a3f9e1c7d5a3b9f2e8c6a4d0b8f3e9c1a
```

#### Why Not Compare Data Directly?

- **Efficiency**: Hash comparison is much faster than row-by-row comparison
- **Simplicity**: Single value comparison is simpler than dealing with data type differences
- **Reproducibility**: Same data always produces same hash, regardless of order

### Row Sorting Strategy

#### Deterministic Ordering

Rows are sorted before hashing to ensure consistency:

```python
# Sort by all columns, treating NULL as empty string
def sort_key(row: Tuple) -> Tuple:
    return tuple('' if val is None else val for val in row)

sorted_rows = sorted(rows, key=sort_key)
```

#### Why Sorting Matters

Without sorting, the same data could produce different hashes if:
- Query results are returned in different order (Postgres vs Snowflake optimizer)
- Indexes are different between databases
- Row insertion order differs

#### Type Handling in Sorting

Mixed-type columns (rare but possible):
```python
# If sorting fails due to mixed types, convert all to strings
def sort_key_str(row: Tuple) -> Tuple:
    return tuple(str(val) if val is not None else '' for val in row)
```

### Data Normalization Pipeline

The normalization process follows this order:

1. **Fetch rows** from database using `SELECT *`
2. **Apply sorting** deterministically by all columns
3. **For each row:**
   - For each column:
     - If NULL: convert to empty string
     - If numeric: round to appropriate precision (2 or 8 decimals)
     - Otherwise: convert to string
   - Join normalized values with `|` delimiter
4. **Join rows** with `\n` delimiter
5. **Compute hash** of final string

Example:
```
Raw rows:
(1, 'ABC', 123.456789, NULL)
(2, 'XYZ', 456.789123, 999.99)

After normalization (assuming numeric column gets 8 decimals):
1|ABC|123.45678901|
2|XYZ|456.78912301|999.99

Joined:
1|ABC|123.45678901|
2|XYZ|456.78912301|999.99

Hash: (SHA256 of above string)
```

### Database-Specific Handling

#### Postgres

- **Column names**: Lowercase, preserved as-is
- **NULL handling**: Standard SQL NULL
- **Numeric types**: NUMERIC, FLOAT, INT all supported
- **Connection pool**: Single persistent connection
- **Query timeout**: Set via `SET statement_timeout` (milliseconds)

#### Snowflake

- **Column names**: Automatically lowercased in cursor description
- **NULL handling**: Standard SQL NULL
- **Numeric types**: DECIMAL, FLOAT, NUMBER all supported
- **Connection pool**: Managed by Snowflake connector
- **Query timeout**: Handled via connection timeouts
- **Warehouse**: COMPUTE_WH (default)
- **Database/Schema**: DBT_DEMO.DEV (defaults, check code)

## Warnings and Gotchas

### Critical Issues

#### 1. Environment Variables Not Exported

**Issue**: Scripts fail with "Missing required environment variables" even though they're in `.env`

**Solution**: You must export variables, not just define them:
```bash
# ✗ WRONG - This doesn't work
source .env

# ✓ CORRECT - Export each variable
export $(cat .env | xargs)

# ✓ ALSO CORRECT - Export manually
export DBT_PG_USER=postgres
export DBT_PG_PASSWORD=pwd
```

#### 2. Model Names Must Match Exactly

**Issue**: MARTS_MODELS list in `generate-report.py` and `compare.py` must be identical

**Watch out for**:
- Case sensitivity differences (PostgreSQL vs Snowflake)
- Typos in model names
- Models added/removed without updating both files

**Verification**:
```bash
# Compare the MARTS_MODELS list in both files
grep -A 12 "MARTS_MODELS = " generate-report.py
grep -A 12 "MARTS_MODELS = " compare.py
# Should be identical
```

#### 3. Schema and Database Defaults

**Issue**: Reports generated from different schemas won't match

**Current defaults**:
- Postgres: Host `localhost`, Port `5433`, Schema `public`
- Snowflake: Database `DBT_DEMO`, Schema `DEV`, Warehouse `COMPUTE_WH`

**If different**:
- Edit line 80-82 and 157-160 in `generate-report.py`
- Ensure both databases have models in same schema

#### 4. dbt Not Run Before Generation

**Issue**: Models don't exist yet, reports are empty or fail

**Solution**: Always run dbt before generating reports
```bash
dbt run
# Wait for completion
# Then:
python3 generate-report.py ...
```

### Data-Related Gotchas

#### 5. NULL Count Differences

**Issue**: Same row count but different hash due to NULL handling

**Example**:
```
Postgres: (col1, col2) = ('A', NULL)
Snowflake: (col1, col2) = ('A', NULL)
# Different internal representation → different hash
```

**Solution**: 
- Check if NULL logic is different
- Use COALESCE/NVL consistently across databases

#### 6. Floating-Point Precision

**Issue**: 0.1 + 0.2 doesn't always equal 0.3

**Solution**: 
- Rounding to 8 decimals helps but isn't perfect
- For critical calculations, use DECIMAL type in dbt

#### 7. Case Sensitivity in Column Names

**Issue**: Postgres column names are case-sensitive with quotes, Snowflake usually case-insensitive

**Solution**:
- Always quote column names in dbt if case matters
- Stick to lowercase for safety

#### 8. Text Whitespace

**Issue**: Extra spaces, tabs, or newlines cause hash mismatch

**Example**: 
```
Postgres: 'ABC  ' (trailing spaces)
Snowflake: 'ABC'   (trimmed)
```

**Solution**: 
- Use TRIM() consistently in both databases
- Or normalize in generate-report.py

### Performance Gotchas

#### 9. Large Models Memory Usage

**Issue**: Models with millions of rows use gigabytes of RAM

**Solution**:
- Run on machine with sufficient RAM (at least 2x model size)
- Process large models during off-peak hours
- Or increase timeout and let it run slower

#### 10. Network Latency

**Issue**: Snowflake queries are slower than Postgres for same data

**Reason**: Network round-trip time to cloud provider

**Solution**: 
- Increase timeout (default 300 seconds)
- Run from same region as Snowflake warehouse
- Or schedule during off-peak hours

### Integration Gotchas

#### 11. Credentials in CI/CD

**Issue**: Never commit `.env` files with real credentials

**Solution**:
- Use CI/CD secrets (GitHub Secrets, Jenkins Credentials)
- Add `.env` to `.gitignore` (already done)
- Use IAM roles if available

#### 12. Exit Code Handling

**Issue**: CI/CD pipeline doesn't fail when validation fails

**Solution**:
```bash
# Your scripts must check exit code:
python3 compare.py ... || exit 1

# And CI/CD must act on it:
# GitHub Actions: "if: failure()"
# Jenkins: "if returnCode != 0 then error"
```

### Maintenance Gotchas

#### 13. Comparing Different Versions

**Issue**: Comparing reports from different dbt/model versions

**Solution**:
- Always compare reports generated on same code version
- Document model changes between baseline and candidate
- Version control your SQL

#### 14. Time Zone Differences

**Issue**: `generated_at` timestamps might differ in timezone

**Solution**:
- All timestamps use UTC (indicated by `Z` suffix)
- This is for documentation only, not compared for validation

---

## Quick Reference

### Common Commands

```bash
# Set up environment
export DBT_PG_USER=postgres
export DBT_PG_PASSWORD=pwd
export DBT_PG_DBNAME=db
export SNOWFLAKE_ACCOUNT=account.region
export SNOWFLAKE_USER=user
export SNOWFLAKE_PASSWORD=pwd

# Generate reports
python3 generate-report.py --dialect postgres --output baseline.json
python3 generate-report.py --dialect snowflake --output candidate.json

# Compare
python3 compare.py --baseline baseline.json --candidate candidate.json --output diff.json

# Check exit code
echo $?
```

### File Structure

```
benchmark/
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── generate-report.py        # Report generator
├── compare.py                # Report comparator
├── baseline/                 # Baseline reports
│   └── report.json
├── candidate/                # Candidate reports
│   └── report.json
└── .env                       # Environment variables (gitignored)
```

### Report Structure

**Generated Report** (JSON):
```json
{
  "generated_at": "2024-01-15T10:25:12Z",
  "dialect": "postgres",
  "database": "bain_analytics",
  "schema": "public",
  "models": {
    "fact_portfolio_summary": {
      "row_count": 15247,
      "data_hash": "a3d5f8c2..."
    }
  }
}
```

**Comparison Diff** (JSON):
```json
{
  "comparison_timestamp": "2024-01-15T10:35:00Z",
  "baseline_metadata": { ... },
  "candidate_metadata": { ... },
  "results": {
    "passed": [ ... ],
    "failed": [ ... ],
    "missing": [ ... ],
    "extra": [ ... ]
  },
  "summary": {
    "total_models": 10,
    "passed": 8,
    "failed": 2,
    "missing": 0,
    "extra": 0
  }
}
```

---

## Support and Debugging

### Enable Verbose Logging

The scripts use Python's standard logging at INFO level. To see more details:

```python
# In generate-report.py or compare.py, change:
logging.basicConfig(level=logging.DEBUG)  # Instead of INFO
```

### Capture Full Output

```bash
# Save stdout and stderr to file
python3 generate-report.py --dialect postgres --output baseline.json \
  > baseline_generation.log 2>&1

# View log
cat baseline_generation.log
```

### Verify Database Access

Before running the full scripts, verify connectivity:

```bash
# Postgres test
python3 -c "
import psycopg2, os
conn = psycopg2.connect(
    host='localhost', port=5433,
    user=os.getenv('DBT_PG_USER'),
    password=os.getenv('DBT_PG_PASSWORD'),
    database=os.getenv('DBT_PG_DBNAME')
)
print('Postgres: OK')
"

# Snowflake test
python3 -c "
from snowflake.connector import connect
conn = connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD')
)
print('Snowflake: OK')
"
```

---

**Last Updated**: January 2024  
**Version**: 1.0
