# Task 23: Compare Baseline vs Candidate Outputs

## Overview

Task 23 is the validation checkpoint in the SQL translation workflow. It compares PostgreSQL baseline data against Snowflake candidate data to verify that all 10 marts models produce identical outputs.

## Status: READY FOR EXECUTION ✅

All implementation files and documentation are complete.

## Quick Links

- **Quick Start**: See `TASK23_QUICKSTART.md` for immediate execution
- **Detailed Guide**: See `TASK23_COMPARISON.md` for comprehensive documentation
- **Main README**: See `README.md` for benchmark system overview

## What Gets Executed

### Automated Scripts

| Script | Platform | Purpose |
|--------|----------|---------|
| `run_comparison_task23.py` | All | Main automation script (Python) |
| `run_task23.sh` | Linux/macOS | Shell wrapper with validation |
| `run_task23.bat` | Windows | Batch wrapper with validation |

### Workflow

1. **Check Prerequisites**
   - Validates environment variables (PostgreSQL + Snowflake)
   - Ensures database connectivity

2. **Generate Baseline Report**
   - Connects to PostgreSQL at localhost:5433
   - Queries all 10 marts models
   - Computes row counts and SHA256 hashes
   - Saves to `baseline/report.json`

3. **Generate Candidate Report**
   - Connects to Snowflake (DBT_DEMO.DEV)
   - Queries all 10 marts models
   - Computes row counts and SHA256 hashes
   - Saves to `candidate/report.json`

4. **Run Comparison**
   - Loads both reports
   - Compares row counts (exact match required)
   - Compares data hashes (exact match required)
   - Generates detailed results
   - Saves to `comparison_diff.json`

5. **Generate Summary**
   - Displays human-readable results
   - Shows PASS/FAIL for each model
   - Provides actionable next steps

## The 10 Models Validated

1. `fact_portfolio_summary` (~15,000 rows)
2. `report_portfolio_overview` (~9,000 rows)
3. `fact_portfolio_pnl` (~23,000 rows)
4. `fact_trade_activity` (~45,000 rows)
5. `report_daily_pnl` (~365 rows)
6. `fact_fund_performance` (~1,250 rows)
7. `fact_cashflow_waterfall` (~567 rows)
8. `fact_portfolio_attribution` (~2,345 rows)
9. `report_ic_dashboard` (~890 rows)
10. `report_lp_quarterly` (~120 rows)

**Total**: ~97,537 rows across all marts

## Prerequisites

### 1. Databases Running

**PostgreSQL:**
- Running at localhost:5433
- All 10 marts models built
- Seeds loaded

**Snowflake:**
- Account accessible (PZXTMSC-WP48482 or your account)
- All 10 marts models built
- Seeds loaded

### 2. Environment Variables

**Required Variables (6 total):**

| Variable | Example | Database |
|----------|---------|----------|
| DBT_PG_USER | `postgres` | PostgreSQL |
| DBT_PG_PASSWORD | `your_password` | PostgreSQL |
| DBT_PG_DBNAME | `bain_analytics` | PostgreSQL |
| SNOWFLAKE_ACCOUNT | `PZXTMSC-WP48482` | Snowflake |
| SNOWFLAKE_USER | `your_user` | Snowflake |
| SNOWFLAKE_PASSWORD | `your_password` | Snowflake |

### 3. Python Dependencies

```bash
cd benchmark/
pip install -r requirements.txt
```

Required:
- `psycopg2-binary`
- `snowflake-connector-python`
- `colorama` (optional)

## Execution

### Recommended: Automated Script

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

**Python (cross-platform):**
```bash
cd benchmark/
python3 run_comparison_task23.py
```

### Alternative: Manual Steps

```bash
cd benchmark/

# Step 1: Baseline
python3 generate-report.py --dialect postgres --output baseline/report.json

# Step 2: Candidate
python3 generate-report.py --dialect snowflake --output candidate/report.json

# Step 3: Compare
python3 compare.py \
  --baseline baseline/report.json \
  --candidate candidate/report.json \
  --output comparison_diff.json
```

## Expected Execution Time

- **Baseline generation**: 30-60 seconds
- **Candidate generation**: 30-60 seconds
- **Comparison**: < 5 seconds
- **Total**: 1-2 minutes

Larger marts (e.g., `fact_trade_activity` with 45K+ rows) take longer to process.

## Success Criteria

Task 23 succeeds when:

- ✅ All environment variables present
- ✅ Both databases accessible
- ✅ baseline/report.json created (10 models)
- ✅ candidate/report.json created (10 models)
- ✅ comparison_diff.json created
- ✅ All 10 models show PASS status
- ✅ Row counts match exactly
- ✅ Data hashes match exactly

## Outputs

### Files Created

| File | Size | Description |
|------|------|-------------|
| `baseline/report.json` | ~3 KB | PostgreSQL ground truth |
| `candidate/report.json` | ~3 KB | Snowflake outputs |
| `comparison_diff.json` | ~5 KB | Detailed comparison results |

### Console Output

**Success Example:**
```
✓ PASS: fact_portfolio_summary (15247 rows, hash match)
✓ PASS: report_portfolio_overview (8934 rows, hash match)
...
Summary: Passed: 10, Failed: 0
✓ SUCCESS: All 10 marts models PASSED validation!
```

**Failure Example:**
```
✗ FAIL: fact_trade_activity
        row_count mismatch: 45123 vs 45120
✗ FAIL: fact_portfolio_pnl
        data_hash mismatch
Summary: Passed: 8, Failed: 2
✗ FAILURE: Some models did not pass validation.
```

## Understanding Failures

### Row Count Mismatch

**Indicates:**
- Missing or extra rows
- WHERE clause differences
- Boolean literal issues (true vs TRUE)
- JOIN condition differences

**Action:**
Review SQL WHERE clauses and JOINs in snowflake2/ models.

### Hash Mismatch (Same Row Count)

**Indicates:**
- Data content differences
- Numeric precision/rounding issues
- Date formatting differences
- NULL handling variations

**Action:**
Review numeric calculations, date functions, and NULL handling.

### Missing Model

**Indicates:**
- Model not built in candidate database
- dbt run failed for this model

**Action:**
Run `dbt run --select model_name` in snowflake2/

## Next Steps

### If All Models Pass ✅

**Congratulations!** SQL translation is validated.
- All 10 marts produce identical outputs
- Row counts match exactly
- Data hashes match exactly
- Translation is correct

**Next Action:**
- Proceed with deployment
- Move to next task in sequence

### If Some Models Fail ❌

**Iteration required** (Task #34)

**Process:**
1. Review failure details in comparison output
2. Identify root cause:
   - Row count mismatch → WHERE/JOIN issues
   - Hash mismatch → Data calculation issues
3. Fix SQL in snowflake2/models/
4. Re-run affected models:
   ```bash
   cd snowflake2/
   dbt run --select model_name+
   ```
5. Regenerate candidate report:
   ```bash
   cd benchmark/
   python3 generate-report.py --dialect snowflake --output candidate/report.json
   ```
6. Re-run comparison:
   ```bash
   python3 compare.py --baseline baseline/report.json --candidate candidate/report.json
   ```
7. Repeat until all models pass

## Common Issues

### Issue: Missing Environment Variables

**Error:**
```
✗ Missing required environment variables: DBT_PG_USER, SNOWFLAKE_ACCOUNT
```

**Fix:**
Export all 6 required variables (see Prerequisites).

### Issue: PostgreSQL Connection Failed

**Error:**
```
Failed to connect to Postgres: connection refused
```

**Fix:**
```bash
cd postgres/
docker-compose up -d
docker-compose ps  # verify running
```

### Issue: Snowflake Connection Failed

**Error:**
```
Failed to connect to Snowflake: Invalid account identifier
```

**Fix:**
- Verify account format (e.g., PZXTMSC-WP48482)
- Check credentials
- Ensure network access

### Issue: Model Not Found

**Error:**
```
Object 'fact_portfolio_summary' does not exist
```

**Fix:**
```bash
cd postgres/  # or snowflake2/
dbt run --select fact_portfolio_summary
```

### Issue: Zero Row Count

**Error:**
```
row_count mismatch: 0 vs 15247
```

**Fix:**
```bash
cd postgres/  # or snowflake2/
dbt seed
dbt run
```

## Technical Details

### Data Normalization

Both reports use identical normalization:
- **Currency columns** (amount, value, mv, pnl, commission): 2 decimals
- **Rate columns** (rate, return, pct, percentage): 8 decimals
- **Other numeric columns**: 8 decimals
- **NULL values**: Empty strings
- **Row sorting**: Deterministic (all columns)
- **Hash algorithm**: SHA256

### Comparison Algorithm

1. Load both JSON reports
2. Validate structure and metadata
3. For each of 10 models:
   - Check if exists in both reports
   - Compare `row_count` field (must be exact)
   - Compare `data_hash` field (must be exact)
4. Categorize results:
   - **passed**: Both counts and hashes match
   - **failed**: Mismatches detected
   - **missing**: In baseline, not in candidate
   - **extra**: In candidate, not in baseline
5. Generate human-readable and JSON outputs

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success (all models passed) |
| 1 | Failure (some models failed or script error) |
| 130 | User interrupted (Ctrl+C) |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Task 23 Workflow                            │
└─────────────────────────────────────────────────────────────────┘

┌──────────────┐                            ┌──────────────┐
│  PostgreSQL  │                            │  Snowflake   │
│  (baseline)  │                            │ (candidate)  │
│              │                            │              │
│  10 marts    │                            │  10 marts    │
│  models      │                            │  models      │
└──────┬───────┘                            └──────┬───────┘
       │                                           │
       │ generate-report.py                        │ generate-report.py
       │ --dialect postgres                        │ --dialect snowflake
       ▼                                           ▼
┌──────────────┐                            ┌──────────────┐
│   baseline/  │                            │  candidate/  │
│  report.json │                            │ report.json  │
│              │                            │              │
│  • row_count │                            │  • row_count │
│  • data_hash │                            │  • data_hash │
└──────┬───────┘                            └──────┬───────┘
       │                                           │
       └───────────────────┬───────────────────────┘
                           │
                           │ compare.py
                           │
                           ▼
                    ┌──────────────┐
                    │ comparison_  │
                    │  diff.json   │
                    │              │
                    │  • passed    │
                    │  • failed    │
                    │  • missing   │
                    │  • extra     │
                    └──────┬───────┘
                           │
                           │ Summary Generation
                           ▼
                    ┌──────────────┐
                    │   Console    │
                    │   Output     │
                    │              │
                    │  ✓ PASS / ✗ FAIL │
                    └──────────────┘
```

## Related Files

### Implementation
- `run_comparison_task23.py` - Main automation script
- `run_task23.sh` - Linux/macOS wrapper
- `run_task23.bat` - Windows wrapper
- `generate-report.py` - Report generation (reused)
- `compare.py` - Comparison logic (reused)

### Documentation
- `TASK23_QUICKSTART.md` - Quick reference
- `TASK23_COMPARISON.md` - Comprehensive guide
- `TASK23_README.md` - This file
- `README.md` - Benchmark system overview
- `BASELINE.md` - PostgreSQL baseline guide
- `TASK22_SUMMARY.md` - Snowflake candidate guide

## Dependencies

### Previous Tasks (Prerequisites)

| Task | Description | Deliverable |
|------|-------------|-------------|
| Task #12 | PostgreSQL baseline capture | All marts built in postgres/ |
| Task #22 | Snowflake candidate generation | All marts built in snowflake2/ |

### Subsequent Tasks (If Failures)

| Task | Description | Purpose |
|------|-------------|---------|
| Task #34 | Fix validation failures | Iterate on failed models |

## Validation Logic

### Row Count Check

```python
if baseline_row_count != candidate_row_count:
    # FAIL: Missing or extra rows
    issues.append(f'row_count mismatch: {baseline_row_count} vs {candidate_row_count}')
```

### Hash Check

```python
if baseline_hash != candidate_hash:
    # FAIL: Data content differs
    issues.append('data_hash mismatch')
```

### Pass Condition

```python
passed = len(issues) == 0
# Both row_count and data_hash must match exactly
```

## FAQ

**Q: What if baseline/candidate reports already exist?**
A: The script will regenerate them to ensure they're current.

**Q: Can I skip baseline generation if I have it from Task #12?**
A: Yes, comment out Step 2 in run_comparison_task23.py if baseline is current.

**Q: How long are the hashes?**
A: SHA256 produces 64-character hexadecimal strings.

**Q: What happens if networks fail mid-execution?**
A: Script will error and exit. Re-run after network is restored.

**Q: Can I run this in CI/CD?**
A: Yes, set environment variables and run the Python script. Exit code 0 = success.

## Best Practices

1. **Fresh Reports**: Always generate fresh reports to avoid stale data
2. **Environment Variables**: Use secure vaults for credentials in production
3. **Review Failures**: Don't ignore hash mismatches - they indicate data issues
4. **Incremental Fixes**: Fix one model at a time and re-validate
5. **Document Issues**: Keep notes on root causes for future reference

## Support

For issues or questions:
1. Review `TASK23_COMPARISON.md` troubleshooting section
2. Check `README.md` for benchmark system overview
3. Verify prerequisites are met
4. Check logs for specific error messages
