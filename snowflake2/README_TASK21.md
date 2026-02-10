# ✅ Task 21: Run Snowflake Models to Build Marts

## 🎯 Task Summary

**Status**: ✅ **READY FOR EXECUTION**  
**Objective**: Execute all translated dbt models in dependency order to materialize 28 views across staging, intermediate, and marts layers in Snowflake.

---

## 📊 Quick Stats

| Metric | Value |
|--------|-------|
| **Total Models** | 28 |
| **Staging Models** | 10 |
| **Intermediate Models** | 8 |
| **Marts Models** | 10 |
| **Target Schema** | DBT_DEMO.DEV |
| **Materialization** | All as VIEWS |
| **Est. Execution Time** | 30-45 seconds |

---

## 🚀 Quick Start

### TL;DR - Execute Now

```bash
# Navigate to snowflake2/
cd snowflake2/

# Set environment variables (if not already set)
export SNOWFLAKE_ACCOUNT='your_account'
export SNOWFLAKE_USER='your_user'
export SNOWFLAKE_PASSWORD='your_password'

# Execute automated script (RECOMMENDED)
chmod +x run_models.sh
./run_models.sh

# OR execute directly
dbt run --profiles-dir .
```

### Verify Success

```bash
# Run verification queries in Snowflake
# Option 1: Copy/paste verify_models.sql sections
# Option 2: Use snowsql
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -f verify_models.sql
```

---

## 📦 Deliverables Created

### 1. **run_models.sh** - Automated Execution Script
- ✅ Environment variable validation
- ✅ Prerequisites checking (dbt installed, files present)
- ✅ Model file counting
- ✅ Colored terminal output with progress
- ✅ Error handling and recovery instructions
- ✅ Success confirmation and next steps

### 2. **verify_models.sql** - Comprehensive Verification
- ✅ View existence checks (all 28 models)
- ✅ Row count validation by layer
- ✅ Data quality checks (no empty views)
- ✅ Sample data preview from key marts
- ✅ Success criteria checklist
- ✅ Summary report with pass/fail status

### 3. **RUN_MODELS_GUIDE.md** - Detailed Documentation
- ✅ Complete prerequisites checklist
- ✅ Full model inventory with descriptions
- ✅ Step-by-step execution instructions
- ✅ Expected output examples
- ✅ Troubleshooting guide for common errors
- ✅ Verification procedures
- ✅ Success criteria validation
- ✅ Execution timeline visualization

### 4. **README_TASK21.md** (This File) - Task Status
- ✅ Quick start commands
- ✅ Verification summary
- ✅ Configuration notes
- ✅ Troubleshooting quick reference

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                 SNOWFLAKE DATA PIPELINE                     │
└─────────────────────────────────────────────────────────────┘

DBT_DEMO.RAW (Seeds)                    [Task 20 - Complete]
    ↓
    ├─ raw_benchmarks (30 rows)
    ├─ raw_cashflows (25 rows)
    ├─ raw_counterparties (10 rows)
    ├─ raw_dates (15 rows)
    ├─ raw_fund_structures (3 rows)
    ├─ raw_instruments (20 rows)
    ├─ raw_portfolios (5 rows)
    ├─ raw_positions (30 rows)
    ├─ raw_trades (30 rows)
    └─ raw_valuations (21 rows)

DBT_DEMO.DEV (Staging)                  [Task 21 - Current]
    ↓
    ├─ stg_benchmarks
    ├─ stg_cashflows
    ├─ stg_counterparties
    ├─ stg_dates
    ├─ stg_fund_structures
    ├─ stg_instruments
    ├─ stg_portfolios
    ├─ stg_positions
    ├─ stg_trades
    └─ stg_valuations

DBT_DEMO.DEV (Intermediate)
    ↓
    ├─ int_benchmark_returns
    ├─ int_cashflow_enriched
    ├─ int_daily_positions
    ├─ int_fund_nav
    ├─ int_irr_calculations
    ├─ int_portfolio_attribution
    ├─ int_trade_enriched
    └─ int_valuation_enriched

DBT_DEMO.DEV (Marts)
    ↓
    ├─ FACTS (6 tables)
    │   ├─ fact_cashflow_waterfall
    │   ├─ fact_fund_performance
    │   ├─ fact_portfolio_attribution
    │   ├─ fact_portfolio_pnl
    │   ├─ fact_portfolio_summary
    │   └─ fact_trade_activity
    │
    └─ REPORTS (4 tables)
        ├─ report_daily_pnl
        ├─ report_ic_dashboard
        ├─ report_lp_quarterly
        └─ report_portfolio_overview
```

---

## ✅ Prerequisites Verification

| Requirement | Status | Command to Verify |
|-------------|--------|-------------------|
| Task 20 Complete (Seeds Loaded) | ✅ | `SELECT COUNT(*) FROM DBT_DEMO.RAW.RAW_BENCHMARKS;` |
| dbt-snowflake Installed | ⚠️ | `dbt --version` |
| Snowflake Connection Active | ⚠️ | `snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER` |
| Environment Variables Set | ⚠️ | `echo $SNOWFLAKE_ACCOUNT` |
| In snowflake2/ Directory | ⚠️ | `ls dbt_project.yml profiles.yml` |
| All Model Files Present | ✅ | `find models/ -name "*.sql" \| wc -l` (should be 28) |

**⚠️ Items require manual verification before execution**

---

## 🎬 Execution Flow

### Automated Script Method (Recommended)

The `run_models.sh` script provides:
- Pre-flight checks for all prerequisites
- Real-time progress monitoring
- Colored output for easy reading
- Error detection and recovery guidance
- Post-execution summary

```bash
./run_models.sh
```

### Direct dbt Command Method

For users who prefer direct control:

```bash
# Run all models
dbt run --profiles-dir .

# Run with debug logging
dbt run --profiles-dir . --debug

# Run specific layer
dbt run --select staging --profiles-dir .
dbt run --select intermediate --profiles-dir .
dbt run --select marts --profiles-dir .

# Run single model and dependents
dbt run --select stg_benchmarks+ --profiles-dir .
```

---

## 🔍 Verification Checklist

After execution, verify success with these checks:

### 1. **dbt Run Output**
```
Expected Final Line:
  Completed successfully
  
Expected Summary:
  Done. PASS=28 WARN=0 ERROR=0 SKIP=0 TOTAL=28
```

### 2. **View Count Query**
```sql
SELECT COUNT(*) AS total_views
FROM DBT_DEMO.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DEV' AND table_type = 'VIEW';
-- Expected: 28
```

### 3. **Layer-by-Layer Verification**
```sql
-- Staging: 10 models
SELECT COUNT(*) FROM DBT_DEMO.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DEV' AND table_name LIKE 'STG_%';

-- Intermediate: 8 models
SELECT COUNT(*) FROM DBT_DEMO.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DEV' AND table_name LIKE 'INT_%';

-- Marts: 10 models (FACT_ + REPORT_)
SELECT COUNT(*) FROM DBT_DEMO.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DEV' 
  AND (table_name LIKE 'FACT_%' OR table_name LIKE 'REPORT_%');
```

### 4. **Data Quality Check**
```sql
-- Ensure no empty views
SELECT * FROM DBT_DEMO.DEV.REPORT_PORTFOLIO_OVERVIEW LIMIT 1;
SELECT * FROM DBT_DEMO.DEV.FACT_PORTFOLIO_SUMMARY LIMIT 1;
SELECT * FROM DBT_DEMO.DEV.REPORT_IC_DASHBOARD LIMIT 1;
```

### 5. **Comprehensive Verification**
```bash
# Run the complete verification script
# (Copy/paste sections from verify_models.sql into Snowflake SQL editor)
```

---

## 🐛 Troubleshooting Quick Reference

### Issue: "Environment variable not set"
```bash
# Solution
export SNOWFLAKE_ACCOUNT='your_account'
export SNOWFLAKE_USER='your_user'
export SNOWFLAKE_PASSWORD='your_password'
```

### Issue: "Seeds not found" (RAW.RAW_* tables missing)
```bash
# Solution: Run Task 20 first
cd snowflake2/
./load_seeds.sh
# OR
dbt seed --profiles-dir .
```

### Issue: "Model compilation error"
```bash
# Solution: Check logs and re-run specific model
dbt run --select failed_model_name --profiles-dir .

# View compiled SQL
cat target/compiled/bain_capital_analytics/models/.../model_name.sql
```

### Issue: "Connection timeout"
```bash
# Solution: Test connection
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER

# Check Snowflake account status
# Verify credentials are correct
```

### Issue: "Dependency not found"
```bash
# Solution: Run dependencies first
dbt run --select +dependent_model --profiles-dir .

# Or run layers in order
dbt run --select staging --profiles-dir .
dbt run --select intermediate --profiles-dir .
dbt run --select marts --profiles-dir .
```

---

## 📊 Success Criteria

### ✅ All Criteria Must Pass

- [ ] **dbt run completes without errors**
  - Final message: "Completed successfully"
  - No ERROR or FAIL statuses in output

- [ ] **All 28 views created in DBT_DEMO.DEV**
  - 10 staging views (STG_*)
  - 8 intermediate views (INT_*)
  - 10 marts views (FACT_*, REPORT_*)

- [ ] **All views are queryable**
  - No "object does not exist" errors
  - Sample queries return data

- [ ] **Views contain data (not empty)**
  - Each view has row_count > 0
  - Matches expected transformation logic

- [ ] **Materialization correct**
  - All table_type = 'VIEW' (not TABLE)
  - Per dbt_project.yml configuration

- [ ] **Target schema correct**
  - All views in DBT_DEMO.DEV
  - No views in wrong schema/database

---

## 📁 File Locations

| File | Purpose | Usage |
|------|---------|-------|
| `run_models.sh` | Automated execution script | `./run_models.sh` |
| `verify_models.sql` | Verification queries | Copy to Snowflake SQL editor |
| `RUN_MODELS_GUIDE.md` | Detailed documentation | Reference guide |
| `dbt_project.yml` | dbt configuration | View materialization settings |
| `profiles.yml` | Snowflake connection | Credentials configuration |
| `models/staging/` | Staging SQL files | 10 .sql files |
| `models/intermediate/` | Intermediate SQL files | 8 .sql files |
| `models/marts/` | Marts SQL files | 10 .sql files |

---

## 🔜 Next Steps After Completion

1. **Run dbt Tests** (Task 22?):
   ```bash
   dbt test --profiles-dir .
   ```

2. **Generate Documentation**:
   ```bash
   dbt docs generate --profiles-dir .
   dbt docs serve --port 8080
   ```

3. **Generate Candidate Report** (likely next task):
   - Compare Snowflake outputs with BigQuery originals
   - Document translation accuracy
   - Identify any data discrepancies

4. **Query Final Marts**:
   ```sql
   -- Portfolio Overview Dashboard
   SELECT * FROM DBT_DEMO.DEV.REPORT_PORTFOLIO_OVERVIEW;
   
   -- Investment Committee Dashboard
   SELECT * FROM DBT_DEMO.DEV.REPORT_IC_DASHBOARD;
   
   -- Limited Partner Quarterly Report
   SELECT * FROM DBT_DEMO.DEV.REPORT_LP_QUARTERLY;
   ```

---

## 📈 Expected Execution Timeline

```
[00:00] 🚀 Start execution
[00:01] ✅ Pre-flight checks complete
[00:02] 📦 Staging layer begins (10 models)
[00:10] 🔧 Intermediate layer begins (8 models)
[00:25] 📊 Marts layer begins (10 models)
[00:45] ✅ All models complete
[00:46] 🔍 Verification recommended
```

---

## 📝 Notes

### Model Count Clarification
The task description mentions 29 models (11 staging + 8 intermediate + 10 marts), but actual file count shows:
- **Staging**: 10 models (not 11)
- **Intermediate**: 8 models ✓
- **Marts**: 10 models ✓
- **Total**: 28 models

All model files are confirmed present and accounted for.

### Schema Configuration
- **Seeds**: Loaded to DBT_DEMO.RAW (Task 20)
- **Models**: All layers target DBT_DEMO.DEV (this task)
- **Materialization**: All as VIEWS (per dbt_project.yml lines 25-29)

### Dependency Handling
dbt automatically resolves model dependencies via the `{{ ref('model_name') }}` function. Models are executed in the correct order:
1. Staging models (reference seeds)
2. Intermediate models (reference staging)
3. Marts models (reference intermediate + staging)

---

## ✅ Task Completion Confirmation

When you see these indicators, Task 21 is complete:

1. ✅ `./run_models.sh` shows: **"🎉 TASK 21 COMPLETE 🎉"**
2. ✅ `dbt run` output shows: **"Completed successfully"**
3. ✅ Verification query returns: **"✅ SUCCESS: All 28 models created"**
4. ✅ All marts are queryable and contain data

---

## 📚 Documentation Reference

- **Quick Start**: See "Quick Start" section above
- **Detailed Guide**: See `RUN_MODELS_GUIDE.md`
- **Verification**: See `verify_models.sql`
- **Troubleshooting**: See `RUN_MODELS_GUIDE.md` Troubleshooting section

---

**Task Status**: ✅ **READY FOR EXECUTION**  
**Prepared By**: Artemis Code Assistant  
**Task Number**: 21/25  
**Dependencies**: Task 20 (Seeds) must be complete  
**Next Task**: Task 22 (Generate Candidate Report)

---

## 🎯 Execute Command

```bash
cd snowflake2/ && ./run_models.sh
```

**Good luck! 🚀**
