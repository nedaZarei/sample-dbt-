-- ============================================================================
-- Snowflake Seed Verification Script
-- ============================================================================
-- Purpose: Verify that all seed tables were loaded correctly into Snowflake
-- Usage: Run this script in Snowflake after executing 'dbt seed'
-- Expected: All 10 tables exist with correct row counts
-- ============================================================================

-- Set context
USE DATABASE DBT_DEMO;
USE SCHEMA RAW;

-- ============================================================================
-- Section 1: Check Table Existence
-- ============================================================================
SHOW TABLES IN DBT_DEMO.RAW;

-- ============================================================================
-- Section 2: Verify Row Counts
-- ============================================================================
SELECT 
    'Seed Table Row Count Verification' AS verification_type,
    CURRENT_TIMESTAMP() AS checked_at;

-- Expected row counts per seed file
WITH expected_counts AS (
    SELECT 'RAW_BENCHMARKS' AS table_name, 30 AS expected_rows
    UNION ALL SELECT 'RAW_CASHFLOWS', 25
    UNION ALL SELECT 'RAW_COUNTERPARTIES', 10
    UNION ALL SELECT 'RAW_DATES', 15
    UNION ALL SELECT 'RAW_FUND_STRUCTURES', 3
    UNION ALL SELECT 'RAW_INSTRUMENTS', 20
    UNION ALL SELECT 'RAW_PORTFOLIOS', 5
    UNION ALL SELECT 'RAW_POSITIONS', 30
    UNION ALL SELECT 'RAW_TRADES', 30
    UNION ALL SELECT 'RAW_VALUATIONS', 21
),
actual_counts AS (
    SELECT 'RAW_BENCHMARKS' AS table_name, COUNT(*) AS actual_rows FROM RAW_BENCHMARKS
    UNION ALL SELECT 'RAW_CASHFLOWS', COUNT(*) FROM RAW_CASHFLOWS
    UNION ALL SELECT 'RAW_COUNTERPARTIES', COUNT(*) FROM RAW_COUNTERPARTIES
    UNION ALL SELECT 'RAW_DATES', COUNT(*) FROM RAW_DATES
    UNION ALL SELECT 'RAW_FUND_STRUCTURES', COUNT(*) FROM RAW_FUND_STRUCTURES
    UNION ALL SELECT 'RAW_INSTRUMENTS', COUNT(*) FROM RAW_INSTRUMENTS
    UNION ALL SELECT 'RAW_PORTFOLIOS', COUNT(*) FROM RAW_PORTFOLIOS
    UNION ALL SELECT 'RAW_POSITIONS', COUNT(*) FROM RAW_POSITIONS
    UNION ALL SELECT 'RAW_TRADES', COUNT(*) FROM RAW_TRADES
    UNION ALL SELECT 'RAW_VALUATIONS', COUNT(*) FROM RAW_VALUATIONS
)
SELECT 
    e.table_name,
    e.expected_rows,
    COALESCE(a.actual_rows, 0) AS actual_rows,
    CASE 
        WHEN COALESCE(a.actual_rows, 0) = e.expected_rows THEN '✓ PASS'
        WHEN COALESCE(a.actual_rows, 0) = 0 THEN '✗ FAIL - TABLE EMPTY'
        ELSE '✗ FAIL - ROW COUNT MISMATCH'
    END AS status
FROM expected_counts e
LEFT JOIN actual_counts a ON e.table_name = a.table_name
ORDER BY e.table_name;

-- ============================================================================
-- Section 3: Data Quality Checks
-- ============================================================================

-- Check for NULL primary keys
SELECT 'NULL Key Check - raw_benchmarks' AS check_name, COUNT(*) AS null_count 
FROM RAW_BENCHMARKS WHERE benchmark_id IS NULL;

SELECT 'NULL Key Check - raw_cashflows' AS check_name, COUNT(*) AS null_count 
FROM RAW_CASHFLOWS WHERE cashflow_id IS NULL;

SELECT 'NULL Key Check - raw_counterparties' AS check_name, COUNT(*) AS null_count 
FROM RAW_COUNTERPARTIES WHERE counterparty_id IS NULL;

SELECT 'NULL Key Check - raw_dates' AS check_name, COUNT(*) AS null_count 
FROM RAW_DATES WHERE date_day IS NULL;

SELECT 'NULL Key Check - raw_fund_structures' AS check_name, COUNT(*) AS null_count 
FROM RAW_FUND_STRUCTURES WHERE fund_id IS NULL;

SELECT 'NULL Key Check - raw_instruments' AS check_name, COUNT(*) AS null_count 
FROM RAW_INSTRUMENTS WHERE instrument_id IS NULL;

SELECT 'NULL Key Check - raw_portfolios' AS check_name, COUNT(*) AS null_count 
FROM RAW_PORTFOLIOS WHERE portfolio_id IS NULL;

SELECT 'NULL Key Check - raw_positions' AS check_name, COUNT(*) AS null_count 
FROM RAW_POSITIONS WHERE position_id IS NULL;

SELECT 'NULL Key Check - raw_trades' AS check_name, COUNT(*) AS null_count 
FROM RAW_TRADES WHERE trade_id IS NULL;

SELECT 'NULL Key Check - raw_valuations' AS check_name, COUNT(*) AS null_count 
FROM RAW_VALUATIONS WHERE valuation_id IS NULL;

-- ============================================================================
-- Section 4: Sample Data Validation
-- ============================================================================

-- Sample from each table to verify data loaded correctly
SELECT 'raw_benchmarks sample' AS table_name, * FROM RAW_BENCHMARKS LIMIT 3;
SELECT 'raw_cashflows sample' AS table_name, * FROM RAW_CASHFLOWS LIMIT 3;
SELECT 'raw_counterparties sample' AS table_name, * FROM RAW_COUNTERPARTIES LIMIT 3;
SELECT 'raw_dates sample' AS table_name, * FROM RAW_DATES LIMIT 3;
SELECT 'raw_fund_structures sample' AS table_name, * FROM RAW_FUND_STRUCTURES LIMIT 3;
SELECT 'raw_instruments sample' AS table_name, * FROM RAW_INSTRUMENTS LIMIT 3;
SELECT 'raw_portfolios sample' AS table_name, * FROM RAW_PORTFOLIOS LIMIT 3;
SELECT 'raw_positions sample' AS table_name, * FROM RAW_POSITIONS LIMIT 3;
SELECT 'raw_trades sample' AS table_name, * FROM RAW_TRADES LIMIT 3;
SELECT 'raw_valuations sample' AS table_name, * FROM RAW_VALUATIONS LIMIT 3;

-- ============================================================================
-- Section 5: Foreign Key Relationship Checks
-- ============================================================================

-- Verify referential integrity (soft checks, no FK constraints)

-- Portfolios should reference valid funds
SELECT 'FK Check: portfolios -> fund_structures' AS check_name,
       COUNT(*) AS orphaned_records
FROM RAW_PORTFOLIOS p
LEFT JOIN RAW_FUND_STRUCTURES f ON p.fund_id = f.fund_id
WHERE f.fund_id IS NULL;

-- Trades should reference valid portfolios
SELECT 'FK Check: trades -> portfolios' AS check_name,
       COUNT(*) AS orphaned_records
FROM RAW_TRADES t
LEFT JOIN RAW_PORTFOLIOS p ON t.portfolio_id = p.portfolio_id
WHERE p.portfolio_id IS NULL;

-- Trades should reference valid instruments
SELECT 'FK Check: trades -> instruments' AS check_name,
       COUNT(*) AS orphaned_records
FROM RAW_TRADES t
LEFT JOIN RAW_INSTRUMENTS i ON t.instrument_id = i.instrument_id
WHERE i.instrument_id IS NULL;

-- Trades should reference valid counterparties
SELECT 'FK Check: trades -> counterparties' AS check_name,
       COUNT(*) AS orphaned_records
FROM RAW_TRADES t
LEFT JOIN RAW_COUNTERPARTIES c ON t.counterparty_id = c.counterparty_id
WHERE c.counterparty_id IS NULL;

-- Positions should reference valid portfolios
SELECT 'FK Check: positions -> portfolios' AS check_name,
       COUNT(*) AS orphaned_records
FROM RAW_POSITIONS pos
LEFT JOIN RAW_PORTFOLIOS p ON pos.portfolio_id = p.portfolio_id
WHERE p.portfolio_id IS NULL;

-- Positions should reference valid instruments
SELECT 'FK Check: positions -> instruments' AS check_name,
       COUNT(*) AS orphaned_records
FROM RAW_POSITIONS pos
LEFT JOIN RAW_INSTRUMENTS i ON pos.instrument_id = i.instrument_id
WHERE i.instrument_id IS NULL;

-- Valuations should reference valid portfolios
SELECT 'FK Check: valuations -> portfolios' AS check_name,
       COUNT(*) AS orphaned_records
FROM RAW_VALUATIONS v
LEFT JOIN RAW_PORTFOLIOS p ON v.portfolio_id = p.portfolio_id
WHERE p.portfolio_id IS NULL;

-- Cashflows should reference valid funds
SELECT 'FK Check: cashflows -> fund_structures' AS check_name,
       COUNT(*) AS orphaned_records
FROM RAW_CASHFLOWS c
LEFT JOIN RAW_FUND_STRUCTURES f ON c.fund_id = f.fund_id
WHERE f.fund_id IS NULL;

-- Cashflows should reference valid portfolios
SELECT 'FK Check: cashflows -> portfolios' AS check_name,
       COUNT(*) AS orphaned_records
FROM RAW_CASHFLOWS c
LEFT JOIN RAW_PORTFOLIOS p ON c.portfolio_id = p.portfolio_id
WHERE p.portfolio_id IS NULL;

-- ============================================================================
-- Section 6: Summary Report
-- ============================================================================

SELECT 
    '========================================' AS report_section,
    'SEED VERIFICATION SUMMARY' AS title,
    '========================================' AS divider;

SELECT 
    'Total Tables' AS metric,
    COUNT(*) AS value
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'RAW' 
  AND TABLE_NAME LIKE 'RAW_%';

SELECT 
    'Total Rows Loaded' AS metric,
    (SELECT COUNT(*) FROM RAW_BENCHMARKS) +
    (SELECT COUNT(*) FROM RAW_CASHFLOWS) +
    (SELECT COUNT(*) FROM RAW_COUNTERPARTIES) +
    (SELECT COUNT(*) FROM RAW_DATES) +
    (SELECT COUNT(*) FROM RAW_FUND_STRUCTURES) +
    (SELECT COUNT(*) FROM RAW_INSTRUMENTS) +
    (SELECT COUNT(*) FROM RAW_PORTFOLIOS) +
    (SELECT COUNT(*) FROM RAW_POSITIONS) +
    (SELECT COUNT(*) FROM RAW_TRADES) +
    (SELECT COUNT(*) FROM RAW_VALUATIONS) AS value;

SELECT 
    'Expected Total Rows' AS metric,
    189 AS value;

-- ============================================================================
-- END OF VERIFICATION SCRIPT
-- ============================================================================
-- 
-- Expected Results:
--   - 10 tables exist in DBT_DEMO.RAW schema
--   - All row counts match expected values
--   - No NULL primary keys
--   - No orphaned foreign key references
--   - Total of 189 rows across all tables
--
-- If all checks pass, the seed loading was successful!
-- ============================================================================
