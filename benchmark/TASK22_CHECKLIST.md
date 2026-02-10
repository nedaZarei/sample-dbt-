# Task 22 Execution Checklist

## Pre-Execution Checklist

### ✅ Prerequisites Verified

- [ ] **Task 21 Complete**: All marts built in Snowflake
  ```sql
  -- Run in Snowflake to verify
  SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
  WHERE table_schema = 'DEV' AND table_type = 'VIEW';
  -- Expected: 10 or more
  ```

- [ ] **Python Installed**: Python 3.7+
  ```bash
  python3 --version
  # Should show: Python 3.7.x or higher
  ```

- [ ] **Dependencies Installed**: Required packages
  ```bash
  pip3 list | grep snowflake-connector-python
  pip3 list | grep psycopg2-binary
  # Or install: pip3 install -r requirements.txt
  ```

- [ ] **Environment Variables Set**: Snowflake credentials
  ```bash
  echo $SNOWFLAKE_ACCOUNT    # Should show: PZXTMSC-WP48482
  echo $SNOWFLAKE_USER       # Should show your username
  echo $SNOWFLAKE_PASSWORD   # Should show (hidden)
  ```

- [ ] **Directory Structure**: candidate/ folder exists
  ```bash
  ls -ld benchmark/candidate/
  # Should exist
  ```

---

## Execution Checklist

### Option 1: Automated Script

- [ ] **Navigate to benchmark directory**
  ```bash
  cd benchmark/
  ```

- [ ] **Make script executable** (Linux/macOS only)
  ```bash
  chmod +x generate_candidate_report.sh
  ```

- [ ] **Run automation script**
  ```bash
  # Linux/macOS
  ./generate_candidate_report.sh
  
  # Windows
  generate_candidate_report.bat
  ```

- [ ] **Monitor output**: Should show connection success and model processing

### Option 2: Direct Python

- [ ] **Navigate to benchmark directory**
  ```bash
  cd benchmark/
  ```

- [ ] **Run generate-report.py**
  ```bash
  python3 generate-report.py --dialect snowflake --output candidate/report.json
  ```

- [ ] **Monitor output**: Should process all 10 models

---

## Post-Execution Verification

### ✅ File Checks

- [ ] **Output file exists**
  ```bash
  ls -lh candidate/report.json
  # Should show file with size > 1KB
  ```

- [ ] **File is valid JSON**
  ```bash
  python3 -m json.tool candidate/report.json > /dev/null
  echo $?
  # Should output: 0
  ```

- [ ] **File size is reasonable**
  ```bash
  stat -f%z candidate/report.json 2>/dev/null || stat -c%s candidate/report.json
  # Should be > 1000 bytes
  ```

### ✅ Content Validation

- [ ] **Run verification script**
  ```bash
  python3 verify_candidate_report.py
  # Should show: All checks passed!
  ```

- [ ] **Check model count**
  ```bash
  python3 -c "import json; print('Models:', len(json.load(open('candidate/report.json'))['models']))"
  # Expected: Models: 10
  ```

- [ ] **Verify no errors**
  ```bash
  python3 -c "import json; data=json.load(open('candidate/report.json')); print('Errors:', [k for k,v in data['models'].items() if 'error' in v])"
  # Expected: Errors: []
  ```

- [ ] **Check metadata**
  ```bash
  python3 -c "import json; data=json.load(open('candidate/report.json')); print('Dialect:', data['dialect']); print('Database:', data['database']); print('Schema:', data['schema'])"
  # Expected:
  # Dialect: snowflake
  # Database: DBT_DEMO
  # Schema: DEV
  ```

### ✅ Data Quality Checks

- [ ] **All models have row counts**
  ```bash
  python3 -c "import json; data=json.load(open('candidate/report.json')); zero_count=[k for k,v in data['models'].items() if v.get('row_count',0)==0]; print('Zero counts:', zero_count)"
  # Expected: Zero counts: []
  ```

- [ ] **All models have valid hashes**
  ```bash
  python3 -c "import json, re; data=json.load(open('candidate/report.json')); invalid=[k for k,v in data['models'].items() if not re.match(r'^[a-f0-9]{64}$', v.get('data_hash',''))]; print('Invalid hashes:', invalid)"
  # Expected: Invalid hashes: []
  ```

- [ ] **Display summary**
  ```bash
  python3 -c "import json; data=json.load(open('candidate/report.json')); print('Summary:'); [print(f'  {k}: {v[\"row_count\"]:,} rows') for k,v in sorted(data['models'].items())]"
  ```

---

## Success Criteria Summary

| Criterion | Status | Verification |
|-----------|--------|--------------|
| Script exits code 0 | ☐ | Check return code |
| `candidate/report.json` exists | ☐ | `ls candidate/report.json` |
| File size > 100 bytes | ☐ | `stat` or `ls -lh` |
| Valid JSON structure | ☐ | `python3 -m json.tool` |
| Contains 10 models | ☐ | Count models in JSON |
| All row_count > 0 | ☐ | Check each model |
| All data_hash valid | ☐ | Regex match SHA256 |
| Dialect = 'snowflake' | ☐ | Check metadata |
| Database = 'DBT_DEMO' | ☐ | Check metadata |
| Schema = 'DEV' | ☐ | Check metadata |

---

## Troubleshooting Quick Reference

| Issue | Quick Fix |
|-------|-----------|
| Missing env vars | `export SNOWFLAKE_ACCOUNT='PZXTMSC-WP48482'` |
| Connection failed | Verify credentials with `snowsql` |
| Module not found | `pip3 install -r requirements.txt` |
| Object doesn't exist | Run `cd snowflake2/ && dbt run --profiles-dir .` |
| Zero row count | Check seeds: `cd snowflake2/ && ./load_seeds.sh` |
| Invalid JSON | Re-run generation script |

---

## Expected Models & Row Counts

| Model | Expected Rows | Purpose |
|-------|---------------|---------|
| fact_portfolio_summary | ~15,000 | Portfolio aggregations |
| report_portfolio_overview | ~9,000 | Portfolio reporting |
| fact_portfolio_pnl | ~23,000 | P&L calculations |
| fact_trade_activity | ~45,000 | Trade transactions |
| report_daily_pnl | ~365 | Daily P&L |
| fact_fund_performance | ~1,250 | Fund metrics |
| fact_cashflow_waterfall | ~567 | Cashflow analysis |
| fact_portfolio_attribution | ~2,345 | Attribution |
| report_ic_dashboard | ~890 | IC dashboard |
| report_lp_quarterly | ~120 | LP quarterly |
| **TOTAL** | **~97,537** | All marts combined |

---

## Next Steps After Completion

### ✅ Task 22 Complete

Once all checks pass:

1. **Verify baseline exists**
   ```bash
   ls -lh baseline/report.json
   # Should exist from earlier task
   ```

2. **Proceed to Task 23**
   ```bash
   cd benchmark/
   python3 compare.py \
     --baseline baseline/report.json \
     --candidate candidate/report.json \
     --output comparison_diff.json
   ```

3. **Review comparison results**
   - Look for PASS/FAIL status
   - Investigate any discrepancies
   - Document findings

---

## Files Generated by This Task

| File | Location | Purpose |
|------|----------|---------|
| `candidate/report.json` | `benchmark/candidate/` | **PRIMARY OUTPUT** |
| `generate_candidate_report.sh` | `benchmark/` | Automation (Unix) |
| `generate_candidate_report.bat` | `benchmark/` | Automation (Windows) |
| `verify_candidate_report.py` | `benchmark/` | Validation tool |
| `TASK22_CANDIDATE_REPORT.md` | `benchmark/` | Full documentation |
| `QUICKSTART_TASK22.md` | `benchmark/` | Quick reference |
| `README_TASK22.md` | `benchmark/` | Overview |
| `TASK22_CHECKLIST.md` | `benchmark/` | This checklist |

---

## Sign-Off

### Execution Completed By:
- **Name**: ___________________
- **Date**: ___________________
- **Exit Code**: ___________________

### Verification Completed By:
- **Name**: ___________________
- **Date**: ___________________
- **All Checks Passed**: ☐ Yes  ☐ No

### Notes:
```
(Any issues, observations, or comments)




```

---

**Task 22 Status**: ⏳ PENDING EXECUTION  
**Ready for Task 23**: ⏳ After completion  
**Blockers**: None (if prerequisites met)
