# ⚡ Quick Start: Run Snowflake Models

**Task 21 - TL;DR Edition**

---

## 🚀 Execute (3 Steps)

```bash
# 1. Set environment variables
export SNOWFLAKE_ACCOUNT='your_account'
export SNOWFLAKE_USER='your_user'  
export SNOWFLAKE_PASSWORD='your_password'

# 2. Navigate and execute
cd snowflake2/
./run_models.sh

# 3. Verify (optional)
# Copy/paste verify_models.sql into Snowflake SQL editor
```

---

## 📊 What Gets Built

| Layer | Count | Location |
|-------|-------|----------|
| 📦 Staging | 10 views | DBT_DEMO.DEV.STG_* |
| 🔧 Intermediate | 8 views | DBT_DEMO.DEV.INT_* |
| 📊 Marts | 10 views | DBT_DEMO.DEV.FACT_*, REPORT_* |
| **TOTAL** | **28 views** | **DBT_DEMO.DEV** |

---

## ✅ Quick Verify

```sql
-- Should return 28
SELECT COUNT(*) FROM DBT_DEMO.INFORMATION_SCHEMA.TABLES
WHERE table_schema = 'DEV' AND table_type = 'VIEW';

-- Sample a key report
SELECT * FROM DBT_DEMO.DEV.REPORT_PORTFOLIO_OVERVIEW LIMIT 5;
```

---

## 🐛 Common Issues

| Issue | Solution |
|-------|----------|
| "Env var not set" | `export SNOWFLAKE_ACCOUNT='...'` |
| "Seeds not found" | Run Task 20: `./load_seeds.sh` |
| "Connection failed" | Verify credentials, test with `snowsql` |
| "Model failed" | `dbt run --select model_name --profiles-dir .` |

---

## 📚 Full Documentation

- **Detailed Guide**: `RUN_MODELS_GUIDE.md`
- **Task Summary**: `README_TASK21.md`
- **Verification**: `verify_models.sql`

---

## ⏱️ Expected Time

**30-45 seconds** total execution time

---

## 🎯 Success = 

✅ Script shows: **"🎉 TASK 21 COMPLETE 🎉"**  
✅ dbt output: **"Completed successfully"**  
✅ 28 views in Snowflake DBT_DEMO.DEV schema
