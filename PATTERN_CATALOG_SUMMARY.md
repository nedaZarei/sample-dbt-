# PostgreSQL to Snowflake SQL Dialect Translation Catalog
## Summary & Quick Reference Guide

**Catalog Version:** 1.0  
**Scope:** 30 SQL files (2 macros, 10 staging models, 8 intermediate models, 10 marts models)  
**Total Unique Patterns:** 12  
**Total Pattern Instances:** 156  

---

## Pattern Summary Table

| ID | Pattern Name | Category | Count | Complexity | Priority |
|----|---|---|---:|:---:|:---:|
| 1 | Date Subtraction | Date/Time | 10 | Low | HIGH |
| 2 | Extract Month | Date/Time | 35 | Low | HIGH |
| 3 | Extract Year | Date/Time | 8 | Low | HIGH |
| 4 | Date Trunc Month | Date/Time | 4 | Low | HIGH |
| 5 | Date Trunc Quarter | Date/Time | 4 | Low | HIGH |
| 6 | Fiscal Quarter Logic | Business Logic | 10 | Medium | HIGH |
| 7 | Fiscal Year Logic | Business Logic | 2 | Medium | HIGH |
| 8 | Row Number Dedup | Window Function | 6 | Medium | MEDIUM |
| 9 | Correlated Subquery | Subquery | 3 | Medium | MEDIUM |
| 10 | Type Casting | Type Operations | 45 | Low | LOW |
| 11 | Unnecessary DISTINCT | Query Optimization | 8 | Low | MEDIUM |
| 12 | Duplicate Window Funcs | Window Optimization | 8 | Medium | MEDIUM |

**Total Instances: 156**

---

## Critical Translation Patterns

### Pattern 1: Date Subtraction (10 instances)
**Problem:** PostgreSQL subtracts dates to get integer days  
**PostgreSQL:**
```sql
({{ end_date }} - {{ start_date }})
cast(settlement_date as date) - cast(trade_date as date)
```

**Snowflake:**
```sql
DATEDIFF(DAY, {{ start_date }}, {{ end_date }})
DATEDIFF(DAY, CAST(trade_date AS DATE), CAST(settlement_date AS DATE))
```

**Files Affected:** 6  
**Locations:** stg_instruments.sql:14, stg_trades.sql:26, date_utils.sql:28, int_irr_calculations.sql:56,68, int_valuation_enriched.sql:48

---

### Pattern 2: Extract Month (35 instances)
**Problem:** PostgreSQL EXTRACT function vs Snowflake MONTH function  
**PostgreSQL:**
```sql
extract(month from {{ date_column }})
```

**Snowflake:**
```sql
MONTH({{ date_column }})
```

**Files Affected:** 11  
**Note:** This appears in nearly every staging and intermediate model, often repeated 4 times per model for fiscal quarter logic

---

### Pattern 3: Extract Year (8 instances)
**Problem:** PostgreSQL EXTRACT function vs Snowflake YEAR function  
**PostgreSQL:**
```sql
extract(year from {{ date_column }})
```

**Snowflake:**
```sql
YEAR({{ date_column }})
```

**Files Affected:** 5  
**Locations:** date_utils.sql, stg_cashflows.sql, stg_dates.sql, fact_cashflow_waterfall.sql, report_portfolio_overview.sql

---

### Patterns 4-5: Date Truncation (8 total)
**Problem:** PostgreSQL date_trunc with lowercase interval vs Snowflake DATE_TRUNC with uppercase

**Pattern 4 - Month (4 instances):**
```sql
-- PostgreSQL
date_trunc('month', {{ date_column }})

-- Snowflake
DATE_TRUNC('MONTH', {{ date_column }})
```

**Pattern 5 - Quarter (4 instances):**
```sql
-- PostgreSQL
date_trunc('quarter', {{ date_column }})

-- Snowflake
DATE_TRUNC('QUARTER', {{ date_column }})
```

**Files Affected:** 4  
**Locations:** stg_dates.sql, fact_trade_activity.sql (2x), report_lp_quarterly.sql (2x)

---

## High-Frequency Patterns: Business Logic (Patterns 6-7)

### Pattern 6: Fiscal Quarter Calculation (10 instances)
**Critical:** This complex CASE statement is **duplicated 10 times** across the codebase and should be consolidated into a Snowflake UDF/macro.

**PostgreSQL (Macro):**
```sql
{% macro get_fiscal_quarter(date_column) %}
    case
        when extract(month from {{ date_column }}) between 1 and 3 then 'Q3'
        when extract(month from {{ date_column }}) between 4 and 6 then 'Q4'
        when extract(month from {{ date_column }}) between 7 and 9 then 'Q1'
        when extract(month from {{ date_column }}) between 10 and 12 then 'Q2'
    end
{% endmacro %}
```

**Snowflake (UDF recommended):**
```sql
CREATE OR REPLACE FUNCTION get_fiscal_quarter(date_col DATE)
RETURNS VARCHAR
LANGUAGE SQL
AS $$
    CASE
        WHEN MONTH(date_col) BETWEEN 1 AND 3 THEN 'Q3'
        WHEN MONTH(date_col) BETWEEN 4 AND 6 THEN 'Q4'
        WHEN MONTH(date_col) BETWEEN 7 AND 9 THEN 'Q1'
        WHEN MONTH(date_col) BETWEEN 10 AND 12 THEN 'Q2'
    END
$$;
```

**Files Using This:** stg_benchmarks, stg_cashflows, stg_portfolios, stg_trades, stg_valuations, int_trade_enriched, fact_cashflow_waterfall, fact_trade_activity, report_ic_dashboard, report_portfolio_overview

---

### Pattern 7: Fiscal Year Calculation (2 instances)
**PostgreSQL (Macro):**
```sql
{% macro get_fiscal_year(date_column) %}
    case
        when extract(month from {{ date_column }}) >= 7
        then extract(year from {{ date_column }}) + 1
        else extract(year from {{ date_column }})
    end
{% endmacro %}
```

**Snowflake (UDF recommended):**
```sql
CREATE OR REPLACE FUNCTION get_fiscal_year(date_col DATE)
RETURNS INTEGER
LANGUAGE SQL
AS $$
    CASE
        WHEN MONTH(date_col) >= 7
        THEN YEAR(date_col) + 1
        ELSE YEAR(date_col)
    END
$$;
```

**Files Using This:** date_utils.sql (macro definition), report_portfolio_overview.sql:25-29

---

## Medium-Priority Patterns

### Pattern 8: Row Number with WHERE Filter (6 instances)
**Anti-Pattern Alert:** PostgreSQL subquery + WHERE clause should use Snowflake QUALIFY

**PostgreSQL:**
```sql
SELECT * FROM (
    SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2 DESC) AS rn
    FROM table
) sub
WHERE rn = 1
```

**Snowflake:**
```sql
SELECT * FROM table
QUALIFY ROW_NUMBER() OVER (PARTITION BY col1 ORDER BY col2 DESC) = 1
```

**Files Affected:** 6  
**Locations:** int_cashflow_enriched.sql, int_trade_enriched.sql, int_valuation_enriched.sql, report_daily_pnl.sql, report_ic_dashboard.sql (2 instances)

---

### Pattern 9: Correlated Subquery (3 instances)
**Anti-Pattern Alert:** Should use window functions instead

**PostgreSQL:**
```sql
WHERE valuation_date = (
    SELECT MAX(v2.valuation_date)
    FROM ref('stg_valuations') v2
    WHERE v2.portfolio_id = v.portfolio_id
)
```

**Snowflake:**
```sql
WHERE valuation_date = MAX(valuation_date) OVER (PARTITION BY portfolio_id)
```

**Files Affected:** 3  
**Locations:** int_irr_calculations.sql, report_lp_quarterly.sql, report_portfolio_overview.sql

---

## Optimization Patterns

### Pattern 10: Type Casting (45 instances)
**Status:** Compatible between PostgreSQL and Snowflake  
**No changes needed** - Both databases support the same CAST syntax

### Pattern 11: Unnecessary DISTINCT (8 instances)
**Optimization Opportunity:** Remove DISTINCT keyword from 8 staging models

**Files with unnecessary DISTINCT:**
- stg_benchmarks.sql:1
- stg_cashflows.sql:3
- stg_counterparties.sql:1
- stg_fund_structures.sql:1
- stg_instruments.sql:2
- stg_portfolios.sql:4
- stg_positions.sql:4
- stg_valuations.sql:2

**Recommendation:** Remove these DISTINCTs as the source data is already unique by primary key

---

### Pattern 12: Duplicate Window Functions (8 instances)
**Optimization Opportunity:** Consolidate repeated window function calculations

**Example from int_fund_nav.sql (lines 54-70):**
```sql
lag(fund_nav) over (partition by fund_id order by valuation_date) is computed 4 times

-- Better approach:
WITH fund_with_lag AS (
    SELECT *,
        lag(fund_nav) over (partition by fund_id order by valuation_date) as prev_fund_nav
    FROM fund_level_nav
)
SELECT *,
    CASE WHEN prev_fund_nav IS NOT NULL AND prev_fund_nav != 0
        THEN (fund_nav - prev_fund_nav) / prev_fund_nav
        ELSE NULL
    END as fund_nav_return,
    fund_nav / NULLIF(committed_capital, 0) as tvpi_gross
FROM fund_with_lag
```

---

## Migration Strategy

### Phase 1: HIGH Priority (Date/Time Operations - Patterns 1-5)
- **Effort:** 2-3 hours
- **Impact:** Affects every model
- **Approach:** Systematic search-and-replace with validation

### Phase 2: HIGH Priority (Business Logic - Patterns 6-7)
- **Effort:** 1-2 hours
- **Impact:** Consolidation reduces duplicate code
- **Approach:** Create Snowflake UDFs, update 10+ models to use them

### Phase 3: MEDIUM Priority (Window Functions - Patterns 8, 12)
- **Effort:** 2-3 hours
- **Impact:** Performance improvement + code cleanliness
- **Approach:** Refactor with QUALIFY clause and optimized CTE structure

### Phase 4: MEDIUM Priority (Subqueries - Pattern 9, 11)
- **Effort:** 1-2 hours
- **Impact:** Performance improvement
- **Approach:** Replace with window functions or remove unnecessary operations

---

## Files by Complexity

### Most Complex Files (Multiple Patterns)
1. **int_fund_nav.sql** - Duplicate window functions (Pattern 12), date operations (Pattern 1)
2. **int_irr_calculations.sql** - Complex business logic, date arithmetic, correlated subquery (Pattern 9)
3. **report_lp_quarterly.sql** - Multiple date_trunc calls, correlated subquery, business logic
4. **fact_cashflow_waterfall.sql** - Repeated fiscal quarter logic, extract calls
5. **int_daily_positions.sql** - Multiple window functions (Pattern 12)

### Simplest Files (Single Pattern Type)
- stg_counterparties.sql (only DISTINCT)
- stg_fund_structures.sql (only DISTINCT)

---

## Verification Checklist

Before considering translation complete, verify:

- [ ] All 10 instances of date subtraction (Pattern 1) converted to DATEDIFF
- [ ] All 35 instances of EXTRACT(MONTH) (Pattern 2) converted to MONTH()
- [ ] All 8 instances of EXTRACT(YEAR) (Pattern 3) converted to YEAR()
- [ ] All 4 instances of date_trunc('month') (Pattern 4) converted to DATE_TRUNC('MONTH')
- [ ] All 4 instances of date_trunc('quarter') (Pattern 5) converted to DATE_TRUNC('QUARTER')
- [ ] 10 fiscal quarter calculations (Pattern 6) replaced with UDF calls
- [ ] 2 fiscal year calculations (Pattern 7) replaced with UDF calls
- [ ] All 6 row_number subqueries (Pattern 8) converted to QUALIFY
- [ ] All 3 correlated subqueries (Pattern 9) converted to window functions
- [ ] 8 unnecessary DISTINCT keywords (Pattern 11) removed
- [ ] Duplicate window functions (Pattern 12) consolidated into intermediate CTEs

---

## Quick Reference: Search Terms

Use these search patterns to find remaining instances in the codebase:

| Pattern | Search Term |
|---------|------------|
| Date subtraction | `as date) -` or `date) - cast` |
| Extract month | `extract(month from` |
| Extract year | `extract(year from` |
| Date truncation | `date_trunc(` |
| Fiscal quarter | `between 1 and 3 then 'Q3'` |
| Row number dedup | `row_number() over` + `where rn = 1` |
| Correlated subquery | `select max` + `where` (in subquery) |
| Unnecessary DISTINCT | `select distinct` (on primary key) |

---

## Impact Summary

| Area | Impact | Effort |
|------|--------|--------|
| Date/Time Operations | Very High (every model affected) | 2-3 hrs |
| Business Logic Consolidation | High (10+ models use duplicated code) | 1-2 hrs |
| Query Optimization | Medium (performance + cleanliness) | 2-3 hrs |
| Type Compatibility | Low (compatible between systems) | 0 hrs |
| **Total Estimated Effort** | **Medium** | **5-8 hrs** |

---

## Documentation Artifacts

1. **PATTERN_CATALOG.json** - Complete structured catalog with all 156 instance locations
2. **PATTERN_CATALOG_SUMMARY.md** - This file, quick reference guide
3. **SQL Files Reference** - All 30 SQL files have been analyzed and documented

---

## Next Steps

1. Review this catalog to understand scope
2. Create Snowflake UDFs for fiscal quarter/year calculations
3. Execute translations in phases (1, 2, 3, 4)
4. Use the verification checklist to confirm completeness
5. Test translated queries in Snowflake environment
