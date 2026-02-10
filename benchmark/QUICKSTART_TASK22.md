# Task 22 Quick Start: Generate Snowflake Candidate Report

## TL;DR

Generate validation report from Snowflake marts for comparison with PostgreSQL baseline.

---

## Execute Now

### Option 1: Automated Script (Recommended)

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

### Option 2: Direct Python Command

```bash
cd benchmark/

# Set credentials (if not already set)
export SNOWFLAKE_ACCOUNT='PZXTMSC-WP48482'
export SNOWFLAKE_USER='your_user'
export SNOWFLAKE_PASSWORD='your_password'

# Generate report
python3 generate-report.py --dialect snowflake --output candidate/report.json
```

---

## What It Does

1. ✅ Connects to Snowflake DBT_DEMO.DEV
2. ✅ Queries all 10 marts models
3. ✅ Computes row counts and SHA256 hashes
4. ✅ Generates `candidate/report.json`

---

## Prerequisites Checklist

- [ ] Task 21 completed (Snowflake marts built)
- [ ] Python 3.7+ installed
- [ ] Dependencies installed (`pip install -r requirements.txt`)
- [ ] Environment variables set:
  - `SNOWFLAKE_ACCOUNT`
  - `SNOWFLAKE_USER`
  - `SNOWFLAKE_PASSWORD`

---

## Expected Output

```
Connecting to snowflake...
Connected to Snowflake account PZXTMSC-WP48482
Processing 10 marts models...
  ✓ fact_portfolio_summary: 15247 rows, hash=a3d5f8c2...
  ✓ report_portfolio_overview: 8934 rows, hash=f2a8c1d5...
  ✓ fact_portfolio_pnl: 23456 rows, hash=b5e2a8d1...
  ... (7 more models)
Report saved to candidate/report.json
```

---

## 10 Marts Models

| # | Model Name | Expected Rows |
|---|------------|---------------|
| 1 | fact_portfolio_summary | ~15,000 |
| 2 | report_portfolio_overview | ~9,000 |
| 3 | fact_portfolio_pnl | ~23,000 |
| 4 | fact_trade_activity | ~45,000 |
| 5 | report_daily_pnl | ~365 |
| 6 | fact_fund_performance | ~1,250 |
| 7 | fact_cashflow_waterfall | ~567 |
| 8 | fact_portfolio_attribution | ~2,345 |
| 9 | report_ic_dashboard | ~890 |
| 10 | report_lp_quarterly | ~120 |

---

## Quick Verification

### Check File Exists
```bash
ls -lh candidate/report.json
# Should show file with size > 1KB
```

### Validate JSON Structure
```bash
cat candidate/report.json | python3 -m json.tool | head -20
```

### Count Models in Report
```bash
python3 -c "import json; print('Models:', len(json.load(open('candidate/report.json'))['models']))"
# Should output: Models: 10
```

---

## Troubleshooting

### "Missing environment variables"
```bash
export SNOWFLAKE_ACCOUNT='PZXTMSC-WP48482'
export SNOWFLAKE_USER='your_user'
export SNOWFLAKE_PASSWORD='your_password'
```

### "Failed to connect"
- Verify credentials are correct
- Check network connectivity
- Ensure warehouse is running

### "Object does not exist"
- Run Task 21 first to build marts
- Verify in Snowflake: `SELECT COUNT(*) FROM DBT_DEMO.DEV.fact_portfolio_summary`

### "Module not found"
```bash
pip3 install -r requirements.txt
```

---

## Success Criteria

✅ Script exits with code 0  
✅ `candidate/report.json` created  
✅ File contains 10 model entries  
✅ Each entry has `row_count` > 0  
✅ Each entry has 64-char `data_hash`

---

## Next Step

**Task 23**: Compare baseline vs candidate

```bash
cd benchmark/
python3 compare.py \
  --baseline baseline/report.json \
  --candidate candidate/report.json \
  --output comparison_diff.json
```

---

## Files Created

| File | Purpose |
|------|---------|
| `candidate/report.json` | Snowflake validation data |
| `generate_candidate_report.sh` | Automation script (Unix) |
| `generate_candidate_report.bat` | Automation script (Windows) |
| `TASK22_CANDIDATE_REPORT.md` | Full documentation |
| `QUICKSTART_TASK22.md` | This quick reference |

---

## Documentation

- **Full Guide**: `TASK22_CANDIDATE_REPORT.md`
- **Script Details**: `generate-report.py` (inline comments)
- **Requirements**: `requirements.txt`

---

**Estimated Time**: 30-60 seconds  
**Output Size**: ~2-5 KB JSON file  
**Dependencies**: Task 21 (Snowflake marts)
