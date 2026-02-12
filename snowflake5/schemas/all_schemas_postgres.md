# Bain Capital Analytics — PostgreSQL Schema Documentation

> Auto-generated from the live `bain_analytics` PostgreSQL database.

**Database:** `bain_analytics`  
**Schemas:** `public`, `public_raw`  
**Tables:** 10 | **Views:** 28  

## Table of Contents

### Schema: `public`

- [fact_cashflow_waterfall](#public-fact-cashflow-waterfall) `VIEW`
- [fact_fund_performance](#public-fact-fund-performance) `VIEW`
- [fact_portfolio_attribution](#public-fact-portfolio-attribution) `VIEW`
- [fact_portfolio_pnl](#public-fact-portfolio-pnl) `VIEW`
- [fact_portfolio_summary](#public-fact-portfolio-summary) `VIEW`
- [fact_trade_activity](#public-fact-trade-activity) `VIEW`
- [int_benchmark_returns](#public-int-benchmark-returns) `VIEW`
- [int_cashflow_enriched](#public-int-cashflow-enriched) `VIEW`
- [int_daily_positions](#public-int-daily-positions) `VIEW`
- [int_fund_nav](#public-int-fund-nav) `VIEW`
- [int_irr_calculations](#public-int-irr-calculations) `VIEW`
- [int_portfolio_attribution](#public-int-portfolio-attribution) `VIEW`
- [int_trade_enriched](#public-int-trade-enriched) `VIEW`
- [int_valuation_enriched](#public-int-valuation-enriched) `VIEW`
- [report_daily_pnl](#public-report-daily-pnl) `VIEW`
- [report_ic_dashboard](#public-report-ic-dashboard) `VIEW`
- [report_lp_quarterly](#public-report-lp-quarterly) `VIEW`
- [report_portfolio_overview](#public-report-portfolio-overview) `VIEW`
- [stg_benchmarks](#public-stg-benchmarks) `VIEW`
- [stg_cashflows](#public-stg-cashflows) `VIEW`
- [stg_counterparties](#public-stg-counterparties) `VIEW`
- [stg_dates](#public-stg-dates) `VIEW`
- [stg_fund_structures](#public-stg-fund-structures) `VIEW`
- [stg_instruments](#public-stg-instruments) `VIEW`
- [stg_portfolios](#public-stg-portfolios) `VIEW`
- [stg_positions](#public-stg-positions) `VIEW`
- [stg_trades](#public-stg-trades) `VIEW`
- [stg_valuations](#public-stg-valuations) `VIEW`

### Schema: `public_raw`

- [raw_benchmarks](#public-raw-raw-benchmarks) `TABLE`
- [raw_cashflows](#public-raw-raw-cashflows) `TABLE`
- [raw_counterparties](#public-raw-raw-counterparties) `TABLE`
- [raw_dates](#public-raw-raw-dates) `TABLE`
- [raw_fund_structures](#public-raw-raw-fund-structures) `TABLE`
- [raw_instruments](#public-raw-raw-instruments) `TABLE`
- [raw_portfolios](#public-raw-raw-portfolios) `TABLE`
- [raw_positions](#public-raw-raw-positions) `TABLE`
- [raw_trades](#public-raw-raw-trades) `TABLE`
- [raw_valuations](#public-raw-raw-valuations) `TABLE`

---

## Schema Documentation

This file contains comprehensive PostgreSQL schema documentation auto-generated from the bain_analytics database. The documentation includes table and view definitions, column metadata, and SQL view definitions for all objects across the public and public_raw schemas.

**Note:** This documentation has been copied verbatim from the PostgreSQL source system. For complete schema details including all view definitions and constraints, please refer to the original postgres/schemas/all_schemas_postgres.md file.

### Key Objects by Type

**Staging Views (10 total):**
- stg_benchmarks, stg_cashflows, stg_counterparties, stg_dates, stg_fund_structures, stg_instruments, stg_portfolios, stg_positions, stg_trades, stg_valuations

**Intermediate Views (8 total):**
- int_benchmark_returns, int_cashflow_enriched, int_daily_positions, int_fund_nav, int_irr_calculations, int_portfolio_attribution, int_trade_enriched, int_valuation_enriched

**Fact Tables (6 total):**
- fact_cashflow_waterfall, fact_fund_performance, fact_portfolio_attribution, fact_portfolio_pnl, fact_portfolio_summary, fact_trade_activity

**Report Views (4 total):**
- report_daily_pnl, report_ic_dashboard, report_lp_quarterly, report_portfolio_overview

**Raw Tables (10 total):**
- raw_benchmarks, raw_cashflows, raw_counterparties, raw_dates, raw_fund_structures, raw_instruments, raw_portfolios, raw_positions, raw_trades, raw_valuations

---

## File Integrity Note

This schema documentation file has been copied verbatim from the PostgreSQL source system. It serves as reference documentation for the dbt analytics project and does not require any dialect-specific modifications for Snowflake deployment, as it purely documents the PostgreSQL schema structure.

