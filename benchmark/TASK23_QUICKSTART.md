# Task 23 Quick Start

## TL;DR

Compare PostgreSQL baseline vs Snowflake candidate to verify identical outputs.

## Prerequisites

Set environment variables:

```bash
# PostgreSQL
export DBT_PG_USER=postgres
export DBT_PG_PASSWORD=your_password
export DBT_PG_DBNAME=bain_analytics

# Snowflake
export SNOWFLAKE_ACCOUNT=PZXTMSC-WP48482
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
```

## Execute

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

**Python:**
```bash
cd benchmark/
python3 run_comparison_task23.py
```

## Expected Result

```
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

Summary:
  Total models: 10
  Passed: 10
  Failed: 0

✓ SUCCESS: All 10 marts models PASSED validation!
```

## Files Generated

- `baseline/report.json` - PostgreSQL ground truth
- `candidate/report.json` - Snowflake outputs
- `comparison_diff.json` - Detailed comparison

## If Failures Occur

Review failure details and proceed to Task #34 for fixes:

```
✗ FAIL: fact_trade_activity
        row_count mismatch: 45123 vs 45120
```

## Manual Steps

If automated script fails:

```bash
cd benchmark/

# Step 1: Generate baseline
python3 generate-report.py --dialect postgres --output baseline/report.json

# Step 2: Generate candidate
python3 generate-report.py --dialect snowflake --output candidate/report.json

# Step 3: Compare
python3 compare.py \
  --baseline baseline/report.json \
  --candidate candidate/report.json \
  --output comparison_diff.json
```

## Troubleshooting

**Missing env vars:** Set all 6 variables listed above

**PostgreSQL connection:** Ensure container running (`docker-compose up -d` in postgres/)

**Snowflake connection:** Verify account identifier and credentials

**Model not found:** Run `dbt run` in postgres/ and snowflake2/

**Zero rows:** Run `dbt seed` then `dbt run` in respective folders

## Documentation

See `TASK23_COMPARISON.md` for detailed documentation.
