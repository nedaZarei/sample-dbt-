# Pattern Catalog by File
## Detailed File-by-File Analysis

---

## MACRO FILES (2 files)

### postgres/macros/date_utils.sql
**Type:** Macro definitions  
**Complexity:** Medium  
**Patterns Identified:** 6  

#### Patterns:
- **Pattern 1** (Line 28): Date Subtraction - `({{ end_date }} - {{ start_date }})`
- **Pattern 2** (Lines 7-10): Extract Month - 4 instances in fiscal quarter macro
- **Pattern 3** (Lines 19-20): Extract Year - 2 instances in fiscal year macro
- **Pattern 6** (Lines 5-12): Fiscal Quarter Calculation - Macro definition (reusable)
- **Pattern 7** (Lines 16-22): Fiscal Year Calculation - Macro definition (reusable)

#### Notes:
- This file defines two important macros: `get_fiscal_quarter` and `get_fiscal_year`
- These macros are **not being used** in the models (anti-pattern)
- The logic from these macros is duplicated manually in 10+ downstream models
- Translation opportunity: Create Snowflake UDFs from these macros

---

### postgres/macros/financial_calculations.sql
**Type:** Macro definitions  
**Complexity:** Low  
**Patterns Identified:** 0

#### Notes:
- Contains macros for return and performance calculations
- No PostgreSQL-specific dialect patterns identified
- Compatible syntax with Snowflake (CASE statements, arithmetic)

---

## STAGING MODELS (10 files)

### postgres/models/staging/stg_benchmarks.sql
**Type:** Staging model  
**Patterns Identified:** 3

#### Patterns:
- **Pattern 2** (Lines 11-14): Extract Month - 4 instances in fiscal quarter calculation
- **Pattern 6** (Lines 10-15): Fiscal Quarter Calculation - Duplicated from macro
- **Pattern 11** (Line 1): Unnecessary DISTINCT - `select distinct` on benchmark_id (PK)

#### Translation Notes:
- Extract month appears 4 times as part of fiscal quarter logic
- Remove `select distinct` on line 1 for optimization
- Replace fiscal quarter logic with call to new Snowflake UDF

---

### postgres/models/staging/stg_cashflows.sql
**Type:** Staging model  
**Patterns Identified:** 3

#### Patterns:
- **Pattern 2** (Lines 15-18): Extract Month - 4 instances in fiscal quarter calculation
- **Pattern 3** (Line 21): Extract Year - `extract(year from cast(cashflow_date as date))`
- **Pattern 6** (Lines 14-19): Fiscal Quarter Calculation - Duplicated
- **Pattern 11** (Line 3): Unnecessary DISTINCT - `select distinct` on cashflow_id (PK)

#### Translation Notes:
- Pattern 3 appears once
- Extract month appears 4 times for fiscal quarter
- Remove `select distinct` on line 3

---

### postgres/models/staging/stg_counterparties.sql
**Type:** Staging model  
**Patterns Identified:** 1

#### Patterns:
- **Pattern 11** (Line 1): Unnecessary DISTINCT - `select distinct` on counterparty_id (PK)

#### Translation Notes:
- Simplest staging model, only optimization needed
- Remove `select distinct` on line 1

---

### postgres/models/staging/stg_dates.sql
**Type:** Staging model  
**Patterns Identified:** 4

#### Patterns:
- **Pattern 2** (Line 12): Extract Month - `extract(month from cast(date_day as date)) as calendar_month`
- **Pattern 3** (Line 13): Extract Year - `extract(year from cast(date_day as date)) as calendar_year`
- **Pattern 4** (Line 10): Date Trunc Month - `date_trunc('month', cast(date_day as date))`
- **Pattern 5** (Line 11): Date Trunc Quarter - `date_trunc('quarter', cast(date_day as date))`

#### Translation Notes:
- Key file for date dimension
- All 4 date/time patterns appear here
- date_trunc requires case change: 'month' -> 'MONTH', 'quarter' -> 'QUARTER'

---

### postgres/models/staging/stg_fund_structures.sql
**Type:** Staging model  
**Patterns Identified:** 1

#### Patterns:
- **Pattern 11** (Line 1): Unnecessary DISTINCT - `select distinct` on fund_id (PK)

#### Translation Notes:
- Simple model, only optimization needed
- Remove `select distinct` on line 1

---

### postgres/models/staging/stg_instruments.sql
**Type:** Staging model  
**Patterns Identified:** 2

#### Patterns:
- **Pattern 1** (Line 14): Date Subtraction - `cast(maturity_date as date) - current_date as days_to_maturity`
- **Pattern 11** (Line 2): Unnecessary DISTINCT - `select distinct` on instrument_id (PK)

#### Translation Notes:
- Date subtraction requires conversion to DATEDIFF
- Remove `select distinct` on line 2

---

### postgres/models/staging/stg_portfolios.sql
**Type:** Staging model  
**Patterns Identified:** 3

#### Patterns:
- **Pattern 2** (Lines 14-17): Extract Month - 4 instances in fiscal quarter calculation
- **Pattern 6** (Lines 13-18): Fiscal Quarter Calculation - Duplicated
- **Pattern 11** (Line 4): Unnecessary DISTINCT - `select distinct` on portfolio_id (PK)

#### Translation Notes:
- Extract month appears 4 times for fiscal quarter
- Remove `select distinct` on line 4

---

### postgres/models/staging/stg_positions.sql
**Type:** Staging model  
**Patterns Identified:** 1

#### Patterns:
- **Pattern 11** (Line 4): Unnecessary DISTINCT - `select distinct` on position_id (PK)

#### Translation Notes:
- Only optimization needed
- Remove `select distinct` on line 4

---

### postgres/models/staging/stg_trades.sql
**Type:** Staging model  
**Patterns Identified:** 3

#### Patterns:
- **Pattern 1** (Line 26): Date Subtraction - `cast(settlement_date as date) - cast(trade_date as date) as settlement_days`
- **Pattern 2** (Lines 20-23): Extract Month - 4 instances in fiscal quarter calculation
- **Pattern 6** (Lines 19-24): Fiscal Quarter Calculation - Duplicated
- **Pattern 11** (Line 4): Unnecessary DISTINCT - `select distinct` on trade_id (PK)

#### Translation Notes:
- Date subtraction on line 26 requires DATEDIFF conversion
- Extract month appears 4 times for fiscal quarter
- Remove `select distinct` on line 4

---

### postgres/models/staging/stg_valuations.sql
**Type:** Staging model  
**Patterns Identified:** 2

#### Patterns:
- **Pattern 2** (Lines 14-17): Extract Month - 4 instances in fiscal quarter calculation
- **Pattern 6** (Lines 13-18): Fiscal Quarter Calculation - Duplicated
- **Pattern 11** (Line 2): Unnecessary DISTINCT - `select distinct` on valuation_id (PK)

#### Translation Notes:
- Extract month appears 4 times for fiscal quarter
- Remove `select distinct` on line 2

---

## INTERMEDIATE MODELS (8 files)

### postgres/models/intermediate/int_benchmark_returns.sql
**Type:** Intermediate model - Benchmark returns with rolling calculations  
**Complexity:** High  
**Patterns Identified:** 1

#### Patterns:
- **Pattern 12** (Lines 27-58): Multiple Identical Window Function Partitions - Rolling window calculations computed multiple times

#### Translation Notes:
- File contains 5 different rolling window calculations (3M, 6M, 12M returns, volatility, avg return)
- Some of these windows could be consolidated
- No critical PostgreSQL-specific dialect patterns

---

### postgres/models/intermediate/int_cashflow_enriched.sql
**Type:** Intermediate model - Enriched cashflow data  
**Complexity:** High  
**Patterns Identified:** 1

#### Patterns:
- **Pattern 8** (Lines 7-30): Row Number with WHERE filter - `row_number() OVER (...) as rn` with `WHERE rn = 1`

#### Translation Notes:
- Use QUALIFY clause in Snowflake instead of subquery + WHERE
- No other critical PostgreSQL patterns

---

### postgres/models/intermediate/int_daily_positions.sql
**Type:** Intermediate model - Daily position snapshots with PnL  
**Complexity:** Very High  
**Patterns Identified:** 2

#### Patterns:
- **Pattern 12** (Lines 64-82): Multiple Identical Window Function Partitions - Several window functions with overlapping partitions

#### Translation Notes:
- Deep CTE nesting (5 levels) - potential optimization opportunity
- Multiple window functions on same partition that could be consolidated
- No critical PostgreSQL dialect patterns

---

### postgres/models/intermediate/int_fund_nav.sql
**Type:** Intermediate model - Fund-level NAV calculation  
**Complexity:** High  
**Patterns Identified:** 1

#### Patterns:
- **Pattern 12** (Lines 54-70): Multiple Identical Window Function Partitions - `lag(fund_nav)` computed 4 times with identical partition

#### Critical Issue:
```sql
lag(fund_nav) over (partition by fund_id order by valuation_date) 
-- computed on line 54, then lines 58, 61, 64 separately
```

#### Translation Notes:
- Consolidate the 4 identical lag() window functions into a single CTE
- This optimization applies to both PostgreSQL and Snowflake

---

### postgres/models/intermediate/int_irr_calculations.sql
**Type:** Intermediate model - IRR calculation  
**Complexity:** Very High  
**Patterns Identified:** 3

#### Patterns:
- **Pattern 1** (Line 56): Date Subtraction - `(ac.cashflow_date - min(ac.cashflow_date) over (...))`
- **Pattern 1** (Line 68): Date Subtraction - `max(cashflow_date) - min(cashflow_date) as investment_days`
- **Pattern 9** (Lines 33-38): Correlated Subquery - Get latest valuation date per portfolio

#### Translation Notes:
- Line 56: Date subtraction with cast to numeric for year fraction calculation
  - `(ac.cashflow_date - min(...))::numeric / 365.25` → `DATEDIFF(DAY, ...) / 365.25::FLOAT`
- Line 68: Date subtraction for investment days
  - `max(cashflow_date) - min(cashflow_date)` → `DATEDIFF(DAY, min(cashflow_date), max(cashflow_date))`
- Line 33-38: Replace correlated subquery with window function

---

### postgres/models/intermediate/int_portfolio_attribution.sql
**Type:** Intermediate model - Portfolio performance attribution  
**Complexity:** Medium  
**Patterns Identified:** 0

#### Translation Notes:
- No critical PostgreSQL-specific patterns
- Can be translated as-is with minimal changes

---

### postgres/models/intermediate/int_trade_enriched.sql
**Type:** Intermediate model - Enriched trade data  
**Complexity:** High  
**Patterns Identified:** 2

#### Patterns:
- **Pattern 6** (Lines 22-27): Fiscal Quarter Calculation - Duplicated from macro
- **Pattern 8** (Lines 5-42): Row Number with WHERE filter - `row_number() OVER (...) as rn` with `WHERE rn = 1`

#### Translation Notes:
- Replace fiscal quarter logic with UDF call
- Use QUALIFY clause instead of subquery + WHERE for deduplication

---

### postgres/models/intermediate/int_valuation_enriched.sql
**Type:** Intermediate model - Enriched valuation data  
**Complexity:** High  
**Patterns Identified:** 2

#### Patterns:
- **Pattern 1** (Line 48): Date Subtraction - `vr.valuation_date - lag(vr.valuation_date) over (...) as days_between_valuations`
- **Pattern 8** (Lines 6-25): Row Number with WHERE filter - `row_number() OVER (...) as rn` with `WHERE rn = 1`
- **Pattern 12** (Lines 30-46): Multiple Identical Window Function Partitions - `lag(net_asset_value)` computed 3 times

#### Translation Notes:
- Line 48: Convert date subtraction to DATEDIFF
- Lines 6-25: Use QUALIFY clause for deduplication
- Lines 30-46: Consolidate 3 identical lag() window functions

---

## MARTS MODELS (10 files)

### postgres/models/marts/fact_cashflow_waterfall.sql
**Type:** Fact table - Cashflow waterfall  
**Complexity:** High  
**Patterns Identified:** 2

#### Patterns:
- **Pattern 2** (Lines 27-30): Extract Month - 4 instances in fiscal quarter calculation
- **Pattern 3** (Line 33): Extract Year - `extract(year from ce.cashflow_date) as calendar_year`
- **Pattern 6** (Lines 26-31): Fiscal Quarter Calculation - Duplicated

#### Translation Notes:
- Extract month appears 4 times for fiscal quarter
- Replace fiscal quarter logic with UDF call

---

### postgres/models/marts/fact_fund_performance.sql
**Type:** Fact table - Fund-level performance  
**Complexity:** Medium  
**Patterns Identified:** 0

#### Translation Notes:
- No critical PostgreSQL-specific dialect patterns
- Can be translated with minimal changes

---

### postgres/models/marts/fact_portfolio_attribution.sql
**Type:** Fact table - Portfolio attribution  
**Complexity:** Medium  
**Patterns Identified:** 0

#### Translation Notes:
- No critical PostgreSQL-specific dialect patterns
- Can be translated with minimal changes

---

### postgres/models/marts/fact_portfolio_pnl.sql
**Type:** Fact table - Portfolio PnL  
**Complexity:** Medium  
**Patterns Identified:** 0

#### Translation Notes:
- No critical PostgreSQL-specific dialect patterns
- Can be translated with minimal changes

---

### postgres/models/marts/fact_portfolio_summary.sql
**Type:** Fact table - Portfolio summary  
**Complexity:** Medium  
**Patterns Identified:** 1

#### Patterns:
- **Pattern 11** (Line 6): Unnecessary DISTINCT - `select distinct` with group by (redundant)

#### Translation Notes:
- Remove `select distinct` on line 6

---

### postgres/models/marts/fact_trade_activity.sql
**Type:** Fact table - Trade activity detail  
**Complexity:** High  
**Patterns Identified:** 3

#### Patterns:
- **Pattern 2** (Lines 39-42): Extract Month - 4 instances in fiscal quarter calculation
- **Pattern 4** (Line 51): Date Trunc Month - `date_trunc('month', t.trade_date)`
- **Pattern 4** (Line 54): Date Trunc Month - `date_trunc('month', t.trade_date)` (2nd instance)
- **Pattern 6** (Lines 38-43): Fiscal Quarter Calculation - Duplicated

#### Translation Notes:
- Extract month appears 4 times for fiscal quarter
- date_trunc appears twice (lines 51, 54)
- Replace fiscal quarter logic with UDF call
- Change date_trunc('month') to DATE_TRUNC('MONTH')

---

### postgres/models/marts/report_daily_pnl.sql
**Type:** Report - Daily PnL  
**Complexity:** Medium  
**Patterns Identified:** 1

#### Patterns:
- **Pattern 8** (Lines 5-18): Row Number with WHERE filter - `row_number() OVER (...) as rn` with `WHERE rn <= 5`

#### Translation Notes:
- Use QUALIFY clause for last N records filtering

---

### postgres/models/marts/report_ic_dashboard.sql
**Type:** Report - Investment Committee dashboard  
**Complexity:** Very High  
**Patterns Identified:** 2

#### Patterns:
- **Pattern 6** (Lines 74-79): Fiscal Quarter Calculation - Duplicated
- **Pattern 2** (Lines 75-78): Extract Month - 4 instances in fiscal quarter calculation
- **Pattern 8** (Lines 6-18): Row Number with WHERE filter - Latest fund performance (1st instance)
- **Pattern 8** (Lines 21-33): Row Number with WHERE filter - Latest portfolio summary (2nd instance)

#### Translation Notes:
- Extract month appears 4 times for fiscal quarter
- Replace fiscal quarter logic with UDF call
- Use QUALIFY clause for 2 row_number deduplications

---

### postgres/models/marts/report_lp_quarterly.sql
**Type:** Report - LP quarterly reporting  
**Complexity:** Very High  
**Patterns Identified:** 3

#### Patterns:
- **Pattern 4** (Line 12): Date Trunc Quarter - First instance
- **Pattern 4** (Line 22): Date Trunc Month - Should be Quarter (potential bug)
- **Pattern 5** (Line 40): Date Trunc Quarter - Third instance
- **Pattern 9** (Lines 39-45): Correlated Subquery - Get quarter-end valuations (potential bug - uses row_number + subquery)

#### Critical Issues:
- Line 12: `date_trunc('quarter', cf.cashflow_date)` - correct
- Line 22: `date_trunc('quarter', cf.cashflow_date)` - uses date_trunc in group by
- Line 40: `date_trunc('quarter', v.valuation_date)` - in left join

#### Translation Notes:
- Multiple date_trunc calls for quarters (need to convert to Snowflake DATE_TRUNC)
- Line 39-45: Replace subquery logic with window function approach
- Verify lines 12, 22, 40 use consistent quarter truncation

---

### postgres/models/marts/report_portfolio_overview.sql
**Type:** Report - Portfolio overview  
**Complexity:** High  
**Patterns Identified:** 3

#### Patterns:
- **Pattern 2** (Lines 20-23): Extract Month - 4 instances in fiscal quarter calculation
- **Pattern 3** (Lines 27-28): Extract Year - 2 instances in fiscal year calculation
- **Pattern 6** (Lines 19-24): Fiscal Quarter Calculation - Duplicated
- **Pattern 7** (Lines 25-29): Fiscal Year Calculation - Duplicated from macro
- **Pattern 9** (Lines 31-36): Correlated Subquery - Get latest position_date per portfolio

#### Translation Notes:
- Extract month appears 4 times for fiscal quarter (lines 20-23)
- Extract year appears 2 times for fiscal year (lines 27-28)
- Replace both fiscal calculations with UDF calls
- Line 31-36: Replace correlated subquery with window function

---

## Summary Statistics by File Type

### Macro Files (2)
- Average patterns per file: 3
- Most common pattern: Pattern 2 (Extract Month)
- Files with critical patterns: 1 (date_utils.sql)

### Staging Models (10)
- Average patterns per file: 2.1
- Most common pattern: Pattern 6 (Fiscal Quarter - appears in 6 files)
- Second most common: Pattern 11 (Unnecessary DISTINCT - appears in 8 files)
- Average complexity: Low-Medium

### Intermediate Models (8)
- Average patterns per file: 1.3
- Most common pattern: Pattern 12 (Duplicate Window Functions - appears in 4 files)
- Average complexity: High
- Files with pattern 8 (Row Number Dedup): 3

### Marts Models (10)
- Average patterns per file: 1.6
- Most common pattern: Pattern 6 (Fiscal Quarter - appears in 4 files)
- Average complexity: High
- Files with correlated subqueries: 2

---

## Translation Complexity Ranking

### Easiest Files (Minimal Changes)
1. stg_counterparties.sql - 1 pattern (remove DISTINCT)
2. stg_fund_structures.sql - 1 pattern (remove DISTINCT)
3. stg_positions.sql - 1 pattern (remove DISTINCT)
4. fact_portfolio_attribution.sql - 0 patterns
5. fact_fund_performance.sql - 0 patterns
6. fact_portfolio_pnl.sql - 0 patterns
7. financial_calculations.sql - 0 patterns

### Most Complex Files (Multiple Pattern Changes)
1. int_irr_calculations.sql - 3 patterns (date subtraction, correlated subquery, year fraction)
2. report_lp_quarterly.sql - 3-4 patterns (date_trunc x3, subquery dedup)
3. report_ic_dashboard.sql - Multiple patterns (fiscal calculations, row_number x2)
4. report_portfolio_overview.sql - 3 patterns (fiscal calc, fiscal year, correlated subquery)
5. int_valuation_enriched.sql - 3 patterns (date subtraction, row_number, duplicate windows)

---

## Files with Duplicate Patterns

### Fiscal Quarter Logic Duplication (Pattern 6 - 10 instances)
Files with this pattern:
1. date_utils.sql (macro definition)
2. stg_benchmarks.sql
3. stg_cashflows.sql
4. stg_portfolios.sql
5. stg_trades.sql
6. stg_valuations.sql
7. int_trade_enriched.sql
8. fact_cashflow_waterfall.sql
9. fact_trade_activity.sql
10. report_ic_dashboard.sql
11. report_portfolio_overview.sql

**Consolidation Opportunity:** Create single Snowflake UDF to eliminate 10 duplicate instances

### Row Number Deduplication (Pattern 8 - 6 instances)
Files with this pattern:
1. int_cashflow_enriched.sql
2. int_trade_enriched.sql
3. int_valuation_enriched.sql
4. report_daily_pnl.sql
5. report_ic_dashboard.sql (2 instances)

**Consolidation Opportunity:** Replace all with QUALIFY clause

---

## Files Requiring UDF Creation

1. **get_fiscal_quarter UDF** - Needed by 10 models
2. **get_fiscal_year UDF** - Needed by 2 models
3. Consider: Performance metrics (return calculation, TVPI calculation) as UDFs

---

## Translation Order Recommendation

### Phase 1: UDF Creation
Create these Snowflake UDFs first (independent, reusable):
- get_fiscal_quarter()
- get_fiscal_year()

### Phase 2: Date/Time Pattern Conversion (HIGH priority)
Files in order of dependency:
1. stg_dates.sql (core date dimension - patterns 2, 3, 4, 5)
2. Other staging files using dates (patterns 1, 2, 3, 6)
3. Intermediate files using dates (patterns 1)
4. Fact/report files using dates (patterns 2, 3, 4, 5, 6)

### Phase 3: Window Function Optimization (MEDIUM priority)
Files with patterns 8, 9, 12:
1. int_valuation_enriched.sql (pattern 1, 8, 12)
2. int_irr_calculations.sql (pattern 1, 9)
3. report_portfolio_overview.sql (pattern 9)
4. fact_daily_positions.sql (pattern 12)
5. int_fund_nav.sql (pattern 12)

### Phase 4: Cleanup (LOW priority)
Remove unnecessary DISTINCTs from 8 staging files

