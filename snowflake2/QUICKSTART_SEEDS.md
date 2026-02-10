# Quick Start: Load Seeds into Snowflake

## TL;DR - Execute Now

```bash
# 1. Set environment variables
export SNOWFLAKE_ACCOUNT="your_account_id"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"

# 2. Navigate to snowflake2 directory
cd snowflake2/

# 3. Run the seed loading script
chmod +x load_seeds.sh
./load_seeds.sh

# OR run dbt seed directly
dbt seed --profiles-dir .
```

## What This Does

Loads 10 CSV files (189 total rows) into `DBT_DEMO.RAW` schema:

| Table | Rows |
|-------|------|
| raw_benchmarks | 30 |
| raw_cashflows | 25 |
| raw_counterparties | 10 |
| raw_dates | 15 |
| raw_fund_structures | 3 |
| raw_instruments | 20 |
| raw_portfolios | 5 |
| raw_positions | 30 |
| raw_trades | 30 |
| raw_valuations | 21 |

## Verify Success

```sql
-- Run in Snowflake
USE DATABASE DBT_DEMO;
USE SCHEMA RAW;
SHOW TABLES;

-- Or run the verification script
-- See: verify_seeds.sql
```

## Need Help?

- **Full Guide**: See `SEED_LOADING_GUIDE.md`
- **Verification Script**: Run `verify_seeds.sql` in Snowflake
- **Automated Script**: Use `load_seeds.sh` for guided execution

## Configuration Files

- `profiles.yml` - Snowflake connection settings
- `dbt_project.yml` - Seed schema configuration (+schema: raw)
- `seeds/*.csv` - 10 source CSV files

## Success Criteria

✅ All 10 tables created in `DBT_DEMO.RAW`  
✅ Row counts match expectations  
✅ No errors or warnings during load  
✅ Tables accessible via `{{ ref('raw_*') }}` in dbt models  

## What's Next?

After seeds are loaded:
1. Run staging models: `dbt run --select staging.*`
2. Run intermediate models: `dbt run --select intermediate.*`
3. Run marts: `dbt run --select marts.*`
4. Run tests: `dbt test`
