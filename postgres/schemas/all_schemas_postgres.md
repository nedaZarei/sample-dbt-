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

## `public`.`fact_cashflow_waterfall`

**Type:** `VIEW` | **Rows:** 25  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `cashflow_id` | `TEXT` | YES |  |  |
| 2 | `fund_id` | `TEXT` | YES |  |  |
| 3 | `fund_name` | `TEXT` | YES |  |  |
| 4 | `fund_type` | `TEXT` | YES |  |  |
| 5 | `portfolio_id` | `TEXT` | YES |  |  |
| 6 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 7 | `cashflow_date` | `DATE` | YES |  |  |
| 8 | `cashflow_type` | `TEXT` | YES |  |  |
| 9 | `amount` | `NUMERIC(18,2)` | YES |  |  |
| 10 | `investor_id` | `TEXT` | YES |  |  |
| 11 | `committed_capital` | `NUMERIC(18,2)` | YES |  |  |
| 12 | `management_fee_rate` | `NUMERIC(8,4)` | YES |  |  |
| 13 | `carry_rate` | `NUMERIC(8,4)` | YES |  |  |
| 14 | `hurdle_rate` | `NUMERIC(8,4)` | YES |  |  |
| 15 | `cumulative_called` | `NUMERIC` | YES |  |  |
| 16 | `cumulative_distributed` | `NUMERIC` | YES |  |  |
| 17 | `cumulative_net_cashflow` | `NUMERIC` | YES |  |  |
| 18 | `pct_called` | `NUMERIC` | YES |  |  |
| 19 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 20 | `calendar_year` | `NUMERIC` | YES |  |  |
| 21 | `net_asset_value` | `NUMERIC(18,2)` | YES |  |  |
| 22 | `gross_asset_value` | `NUMERIC(18,2)` | YES |  |  |
| 23 | `nav_unrealized_pnl` | `NUMERIC(18,2)` | YES |  |  |
| 24 | `nav_realized_pnl` | `NUMERIC(18,2)` | YES |  |  |
| 25 | `estimated_carry` | `NUMERIC` | YES |  |  |
| 26 | `annual_mgmt_fee` | `NUMERIC` | YES |  |  |
| 27 | `dpi` | `NUMERIC` | YES |  |  |
| 28 | `tvpi` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH enriched_cashflows AS (
         SELECT ce.cashflow_id,
            ce.fund_id,
            ce.fund_name,
            ce.fund_type,
            ce.portfolio_id,
            ce.portfolio_name,
            ce.cashflow_date,
            ce.cashflow_type,
            ce.amount,
            ce.investor_id,
            ce.committed_capital,
            ce.management_fee_rate,
            ce.carry_rate,
            ce.hurdle_rate,
            ce.cumulative_called,
            ce.cumulative_distributed,
            ce.cumulative_net_cashflow,
            ce.pct_called,
                CASE
                    WHEN ((EXTRACT(month FROM ce.cashflow_date) >= (1)::numeric) AND (EXTRACT(month FROM ce.cashflow_date) <= (3)::numeric)) THEN 'Q3'::text
                    WHEN ((EXTRACT(month FROM ce.cashflow_date) >= (4)::numeric) AND (EXTRACT(month FROM ce.cashflow_date) <= (6)::numeric)) THEN 'Q4'::text
                    WHEN ((EXTRACT(month FROM ce.cashflow_date) >= (7)::numeric) AND (EXTRACT(month FROM ce.cashflow_date) <= (9)::numeric)) THEN 'Q1'::text
                    WHEN ((EXTRACT(month FROM ce.cashflow_date) >= (10)::numeric) AND (EXTRACT(month FROM ce.cashflow_date) <= (12)::numeric)) THEN 'Q2'::text
                    ELSE NULL::text
                END AS fiscal_quarter,
            EXTRACT(year FROM ce.cashflow_date) AS calendar_year
           FROM int_cashflow_enriched ce
        ), waterfall_with_nav AS (
         SELECT ecf.cashflow_id,
            ecf.fund_id,
            ecf.fund_name,
            ecf.fund_type,
            ecf.portfolio_id,
            ecf.portfolio_name,
            ecf.cashflow_date,
            ecf.cashflow_type,
            ecf.amount,
            ecf.investor_id,
            ecf.committed_capital,
            ecf.management_fee_rate,
            ecf.carry_rate,
            ecf.hurdle_rate,
            ecf.cumulative_called,
            ecf.cumulative_distributed,
            ecf.cumulative_net_cashflow,
            ecf.pct_called,
            ecf.fiscal_quarter,
            ecf.calendar_year,
            v.net_asset_value,
            v.gross_asset_value,
            v.unrealized_pnl AS nav_unrealized_pnl,
            v.realized_pnl AS nav_realized_pnl
           FROM (enriched_cashflows ecf
             LEFT JOIN stg_valuations v ON (((ecf.portfolio_id = v.portfolio_id) AND (ecf.cashflow_date = v.valuation_date))))
        ), waterfall_calculations AS (
         SELECT wn.cashflow_id,
            wn.fund_id,
            wn.fund_name,
            wn.fund_type,
            wn.portfolio_id,
            wn.portfolio_name,
            wn.cashflow_date,
            wn.cashflow_type,
            wn.amount,
            wn.investor_id,
            wn.committed_capital,
            wn.management_fee_rate,
            wn.carry_rate,
            wn.hurdle_rate,
            wn.cumulative_called,
            wn.cumulative_distributed,
            wn.cumulative_net_cashflow,
            wn.pct_called,
            wn.fiscal_quarter,
            wn.calendar_year,
            wn.net_asset_value,
            wn.gross_asset_value,
            wn.nav_unrealized_pnl,
            wn.nav_realized_pnl,
                CASE
                    WHEN (wn.cumulative_distributed > (wn.cumulative_called * ((1)::numeric + wn.hurdle_rate))) THEN ((wn.cumulative_distributed - (wn.cumulative_called * ((1)::numeric + wn.hurdle_rate))) * wn.carry_rate)
                    ELSE (0)::numeric
                END AS estimated_carry,
            (wn.cumulative_called * wn.management_fee_rate) AS annual_mgmt_fee,
                CASE
                    WHEN (wn.cumulative_called > (0)::numeric) THEN (wn.cumulative_distributed / wn.cumulative_called)
                    ELSE (0)::numeric
                END AS dpi,
                CASE
                    WHEN ((wn.cumulative_called > (0)::numeric) AND (wn.net_asset_value IS NOT NULL)) THEN ((wn.cumulative_distributed + wn.net_asset_value) / wn.cumulative_called)
                    ELSE NULL::numeric
                END AS tvpi
           FROM waterfall_with_nav wn
        )
 SELECT cashflow_id,
    fund_id,
    fund_name,
    fund_type,
    portfolio_id,
    portfolio_name,
    cashflow_date,
    cashflow_type,
    amount,
    investor_id,
    committed_capital,
    management_fee_rate,
    carry_rate,
    hurdle_rate,
    cumulative_called,
    cumulative_distributed,
    cumulative_net_cashflow,
    pct_called,
    fiscal_quarter,
    calendar_year,
    net_asset_value,
    gross_asset_value,
    nav_unrealized_pnl,
    nav_realized_pnl,
    estimated_carry,
    annual_mgmt_fee,
    dpi,
    tvpi
   FROM waterfall_calculations;
```

</details>

---

## `public`.`fact_fund_performance`

**Type:** `VIEW` | **Rows:** 12  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 2 | `fund_name` | `TEXT` | YES |  |  |
| 3 | `fund_type` | `TEXT` | YES |  |  |
| 4 | `committed_capital` | `NUMERIC(18,2)` | YES |  |  |
| 5 | `valuation_date` | `DATE` | YES |  |  |
| 6 | `fund_nav` | `NUMERIC` | YES |  |  |
| 7 | `fund_gav` | `NUMERIC` | YES |  |  |
| 8 | `fund_total_liabilities` | `NUMERIC` | YES |  |  |
| 9 | `fund_unrealized_pnl` | `NUMERIC` | YES |  |  |
| 10 | `fund_realized_pnl` | `NUMERIC` | YES |  |  |
| 11 | `fund_nav_return` | `NUMERIC` | YES |  |  |
| 12 | `tvpi_gross` | `NUMERIC` | YES |  |  |
| 13 | `num_portfolios` | `BIGINT` | YES |  |  |
| 14 | `period_calls` | `NUMERIC` | YES |  |  |
| 15 | `period_distributions` | `NUMERIC` | YES |  |  |
| 16 | `period_fees` | `NUMERIC` | YES |  |  |
| 17 | `period_net_cashflow` | `NUMERIC` | YES |  |  |
| 18 | `fund_total_invested` | `NUMERIC` | YES |  |  |
| 19 | `fund_total_distributed` | `NUMERIC` | YES |  |  |
| 20 | `fund_approx_irr` | `NUMERIC` | YES |  |  |
| 21 | `fund_tvpi` | `NUMERIC` | YES |  |  |
| 22 | `fund_dpi` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH fund_nav_data AS (
         SELECT fn.fund_id,
            fn.fund_name,
            fn.fund_type,
            fn.committed_capital,
            fn.valuation_date,
            fn.fund_nav,
            fn.fund_gav,
            fn.fund_total_liabilities,
            fn.fund_unrealized_pnl,
            fn.fund_realized_pnl,
            fn.fund_nav_return,
            fn.tvpi_gross,
            fn.num_portfolios
           FROM int_fund_nav fn
        ), fund_cashflows AS (
         SELECT cf.fund_id,
            cf.cashflow_date,
            sum(
                CASE
                    WHEN (cf.cashflow_type = 'capital_call'::text) THEN cf.amount
                    ELSE (0)::numeric
                END) AS period_calls,
            sum(
                CASE
                    WHEN (cf.cashflow_type = 'distribution'::text) THEN cf.amount
                    ELSE (0)::numeric
                END) AS period_distributions,
            sum(
                CASE
                    WHEN (cf.cashflow_type = 'management_fee'::text) THEN cf.amount
                    ELSE (0)::numeric
                END) AS period_fees,
            sum(cf.amount) AS period_net_cashflow
           FROM stg_cashflows cf
          GROUP BY cf.fund_id, cf.cashflow_date
        ), fund_irr AS (
         SELECT irr.fund_id,
            sum(irr.total_invested) AS fund_total_invested,
            sum(irr.total_distributed) AS fund_total_distributed,
            sum(irr.terminal_value) AS fund_terminal_value,
            avg(irr.approx_irr) AS fund_approx_irr,
            avg(irr.tvpi) AS fund_tvpi,
            avg(irr.dpi) AS fund_dpi
           FROM int_irr_calculations irr
          GROUP BY irr.fund_id
        ), combined AS (
         SELECT fn.fund_id,
            fn.fund_name,
            fn.fund_type,
            fn.committed_capital,
            fn.valuation_date,
            fn.fund_nav,
            fn.fund_gav,
            fn.fund_total_liabilities,
            fn.fund_unrealized_pnl,
            fn.fund_realized_pnl,
            fn.fund_nav_return,
            fn.tvpi_gross,
            fn.num_portfolios,
            COALESCE(fc.period_calls, (0)::numeric) AS period_calls,
            COALESCE(fc.period_distributions, (0)::numeric) AS period_distributions,
            COALESCE(fc.period_fees, (0)::numeric) AS period_fees,
            COALESCE(fc.period_net_cashflow, (0)::numeric) AS period_net_cashflow,
            fi.fund_total_invested,
            fi.fund_total_distributed,
            fi.fund_approx_irr,
            fi.fund_tvpi,
            fi.fund_dpi
           FROM ((fund_nav_data fn
             LEFT JOIN fund_cashflows fc ON ((((fn.fund_id)::text = fc.fund_id) AND (fn.valuation_date = fc.cashflow_date))))
             LEFT JOIN fund_irr fi ON (((fn.fund_id)::text = fi.fund_id)))
        )
 SELECT fund_id,
    fund_name,
    fund_type,
    committed_capital,
    valuation_date,
    fund_nav,
    fund_gav,
    fund_total_liabilities,
    fund_unrealized_pnl,
    fund_realized_pnl,
    fund_nav_return,
    tvpi_gross,
    num_portfolios,
    period_calls,
    period_distributions,
    period_fees,
    period_net_cashflow,
    fund_total_invested,
    fund_total_distributed,
    fund_approx_irr,
    fund_tvpi,
    fund_dpi
   FROM combined;
```

</details>

---

## `public`.`fact_portfolio_attribution`

**Type:** `VIEW` | **Rows:** 22  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `portfolio_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 3 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 4 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 5 | `position_date` | `DATE` | YES |  |  |
| 6 | `sector` | `TEXT` | YES |  |  |
| 7 | `sector_mv` | `NUMERIC` | YES |  |  |
| 8 | `sector_ugl` | `NUMERIC` | YES |  |  |
| 9 | `num_instruments` | `BIGINT` | YES |  |  |
| 10 | `total_portfolio_mv` | `NUMERIC` | YES |  |  |
| 11 | `total_portfolio_ugl` | `NUMERIC` | YES |  |  |
| 12 | `sector_weight` | `NUMERIC` | YES |  |  |
| 13 | `sector_contribution` | `NUMERIC` | YES |  |  |
| 14 | `benchmark_name` | `TEXT` | YES |  |  |
| 15 | `benchmark_return` | `NUMERIC(18,8)` | YES |  |  |
| 16 | `benchmark_3m_return` | `NUMERIC` | YES |  |  |
| 17 | `benchmark_12m_return` | `NUMERIC` | YES |  |  |
| 18 | `sector_active_return` | `NUMERIC` | YES |  |  |
| 19 | `portfolio_return` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH attribution AS (
         SELECT pa.portfolio_id,
            pa.portfolio_name,
            pa.strategy,
            pa.fund_id,
            pa.position_date,
            pa.sector,
            pa.sector_mv,
            pa.sector_ugl,
            pa.num_instruments,
            pa.total_portfolio_mv,
            pa.total_portfolio_ugl,
            pa.sector_weight,
            pa.sector_contribution
           FROM int_portfolio_attribution pa
        ), attribution_with_benchmark AS (
         SELECT a.portfolio_id,
            a.portfolio_name,
            a.strategy,
            a.fund_id,
            a.position_date,
            a.sector,
            a.sector_mv,
            a.sector_ugl,
            a.num_instruments,
            a.total_portfolio_mv,
            a.total_portfolio_ugl,
            a.sector_weight,
            a.sector_contribution,
            bm.benchmark_name,
            bm.return_mtd AS benchmark_return,
            bm.rolling_3m_return AS benchmark_3m_return,
            bm.rolling_12m_return AS benchmark_12m_return
           FROM (attribution a
             LEFT JOIN int_benchmark_returns bm ON (((a.position_date = bm.benchmark_date) AND (bm.benchmark_id = 'BM_SP500'::text))))
        ), attribution_with_alpha AS (
         SELECT ab.portfolio_id,
            ab.portfolio_name,
            ab.strategy,
            ab.fund_id,
            ab.position_date,
            ab.sector,
            ab.sector_mv,
            ab.sector_ugl,
            ab.num_instruments,
            ab.total_portfolio_mv,
            ab.total_portfolio_ugl,
            ab.sector_weight,
            ab.sector_contribution,
            ab.benchmark_name,
            ab.benchmark_return,
            ab.benchmark_3m_return,
            ab.benchmark_12m_return,
                CASE
                    WHEN (ab.total_portfolio_mv <> (0)::numeric) THEN ((ab.sector_ugl / ab.total_portfolio_mv) - (COALESCE(ab.benchmark_return, (0)::numeric) * ab.sector_weight))
                    ELSE (0)::numeric
                END AS sector_active_return,
                CASE
                    WHEN (ab.total_portfolio_mv <> (0)::numeric) THEN (ab.total_portfolio_ugl / ab.total_portfolio_mv)
                    ELSE (0)::numeric
                END AS portfolio_return
           FROM attribution_with_benchmark ab
        )
 SELECT portfolio_id,
    portfolio_name,
    strategy,
    fund_id,
    position_date,
    sector,
    sector_mv,
    sector_ugl,
    num_instruments,
    total_portfolio_mv,
    total_portfolio_ugl,
    sector_weight,
    sector_contribution,
    benchmark_name,
    benchmark_return,
    benchmark_3m_return,
    benchmark_12m_return,
    sector_active_return,
    portfolio_return
   FROM attribution_with_alpha;
```

</details>

---

## `public`.`fact_portfolio_pnl`

**Type:** `VIEW` | **Rows:** 12  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `portfolio_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 3 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 4 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 5 | `pnl_date` | `DATE` | YES |  |  |
| 6 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 7 | `fiscal_year` | `TEXT` | YES |  |  |
| 8 | `position_daily_pnl` | `NUMERIC` | YES |  |  |
| 9 | `total_market_value` | `NUMERIC` | YES |  |  |
| 10 | `num_positions` | `BIGINT` | YES |  |  |
| 11 | `total_traded_notional` | `NUMERIC` | YES |  |  |
| 12 | `total_commissions` | `NUMERIC` | YES |  |  |
| 13 | `num_trades` | `BIGINT` | YES |  |  |
| 14 | `cumulative_pnl` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH trade_pnl AS (
         SELECT t.portfolio_id,
            t.trade_date,
            t.instrument_id,
            t.trade_type,
            t.notional_amount,
            t.commission,
            i.instrument_name,
            i.instrument_type,
            i.sector,
            p.portfolio_name,
            p.strategy,
            p.fund_id,
            d.fiscal_quarter,
            d.fiscal_year,
            d.is_month_end,
            d.is_quarter_end
           FROM (((stg_trades t
             JOIN stg_instruments i ON ((t.instrument_id = i.instrument_id)))
             JOIN stg_portfolios p ON ((t.portfolio_id = (p.portfolio_id)::text)))
             JOIN stg_dates d ON ((t.trade_date = d.date_day)))
        ), position_pnl AS (
         SELECT dp.portfolio_id,
            dp.position_date,
            dp.portfolio_name,
            dp.strategy,
            dp.fund_id,
            dp.fiscal_quarter,
            dp.fiscal_year,
            sum(dp.daily_pnl) AS position_daily_pnl,
            sum(dp.market_value) AS total_market_value,
            count(DISTINCT dp.instrument_id) AS num_positions
           FROM int_daily_positions dp
          GROUP BY dp.portfolio_id, dp.position_date, dp.portfolio_name, dp.strategy, dp.fund_id, dp.fiscal_quarter, dp.fiscal_year
        ), trade_activity AS (
         SELECT trade_pnl.portfolio_id,
            trade_pnl.trade_date,
            sum(trade_pnl.notional_amount) AS total_traded_notional,
            sum(trade_pnl.commission) AS total_commissions,
            count(*) AS num_trades
           FROM trade_pnl
          GROUP BY trade_pnl.portfolio_id, trade_pnl.trade_date
        )
 SELECT pp.portfolio_id,
    pp.portfolio_name,
    pp.strategy,
    pp.fund_id,
    pp.position_date AS pnl_date,
    pp.fiscal_quarter,
    pp.fiscal_year,
    pp.position_daily_pnl,
    pp.total_market_value,
    pp.num_positions,
    COALESCE(ta.total_traded_notional, (0)::numeric) AS total_traded_notional,
    COALESCE(ta.total_commissions, (0)::numeric) AS total_commissions,
    COALESCE(ta.num_trades, (0)::bigint) AS num_trades,
    sum(pp.position_daily_pnl) OVER (PARTITION BY pp.portfolio_id ORDER BY pp.position_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pnl
   FROM (position_pnl pp
     LEFT JOIN trade_activity ta ON (((pp.portfolio_id = ta.portfolio_id) AND (pp.position_date = ta.trade_date))));
```

</details>

---

## `public`.`fact_portfolio_summary`

**Type:** `VIEW` | **Rows:** 12  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `portfolio_id` | `VARCHAR(10)` | YES |  |  |
| 2 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 3 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 4 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 5 | `manager_name` | `VARCHAR(100)` | YES |  |  |
| 6 | `position_date` | `DATE` | YES |  |  |
| 7 | `num_positions` | `BIGINT` | YES |  |  |
| 8 | `total_market_value` | `NUMERIC` | YES |  |  |
| 9 | `total_cost_basis` | `NUMERIC` | YES |  |  |
| 10 | `total_unrealized_pnl` | `NUMERIC` | YES |  |  |
| 11 | `portfolio_return_pct` | `NUMERIC` | YES |  |  |
| 12 | `largest_position_mv` | `NUMERIC` | YES |  |  |
| 13 | `smallest_position_mv` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT p.portfolio_id,
    p.portfolio_name,
    p.strategy,
    p.fund_id,
    p.manager_name,
    pos.position_date,
    count(DISTINCT pos.instrument_id) AS num_positions,
    sum(pos.market_value) AS total_market_value,
    sum(pos.cost_basis) AS total_cost_basis,
    sum(pos.unrealized_gain_loss) AS total_unrealized_pnl,
        CASE
            WHEN (sum(pos.cost_basis) <> (0)::numeric) THEN (sum(pos.unrealized_gain_loss) / abs(sum(pos.cost_basis)))
            ELSE (0)::numeric
        END AS portfolio_return_pct,
    max(pos.market_value) AS largest_position_mv,
    min(pos.market_value) AS smallest_position_mv
   FROM (stg_portfolios p
     JOIN stg_positions pos ON (((p.portfolio_id)::text = pos.portfolio_id)))
  WHERE (p.is_active = true)
  GROUP BY p.portfolio_id, p.portfolio_name, p.strategy, p.fund_id, p.manager_name, pos.position_date;
```

</details>

---

## `public`.`fact_trade_activity`

**Type:** `VIEW` | **Rows:** 0  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `trade_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `instrument_id` | `TEXT` | YES |  |  |
| 4 | `counterparty_id` | `TEXT` | YES |  |  |
| 5 | `trade_date` | `DATE` | YES |  |  |
| 6 | `settlement_date` | `DATE` | YES |  |  |
| 7 | `trade_type` | `TEXT` | YES |  |  |
| 8 | `quantity` | `NUMERIC(18,4)` | YES |  |  |
| 9 | `price` | `NUMERIC(18,6)` | YES |  |  |
| 10 | `notional_amount` | `NUMERIC(18,2)` | YES |  |  |
| 11 | `commission` | `NUMERIC(18,2)` | YES |  |  |
| 12 | `currency` | `TEXT` | YES |  |  |
| 13 | `settlement_days` | `INTEGER` | YES |  |  |
| 14 | `instrument_name` | `TEXT` | YES |  |  |
| 15 | `instrument_type` | `TEXT` | YES |  |  |
| 16 | `sector` | `TEXT` | YES |  |  |
| 17 | `liquidity_class` | `TEXT` | YES |  |  |
| 18 | `counterparty_name` | `TEXT` | YES |  |  |
| 19 | `counterparty_type` | `TEXT` | YES |  |  |
| 20 | `credit_rating` | `TEXT` | YES |  |  |
| 21 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 22 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 23 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 24 | `manager_name` | `VARCHAR(100)` | YES |  |  |
| 25 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 26 | `fiscal_year` | `TEXT` | YES |  |  |
| 27 | `is_month_end` | `BOOLEAN` | YES |  |  |
| 28 | `computed_fiscal_quarter` | `TEXT` | YES |  |  |
| 29 | `running_notional` | `NUMERIC` | YES |  |  |
| 30 | `monthly_trade_count` | `BIGINT` | YES |  |  |
| 31 | `monthly_commissions` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT t.trade_id,
    t.portfolio_id,
    t.instrument_id,
    t.counterparty_id,
    t.trade_date,
    t.settlement_date,
    t.trade_type,
    t.quantity,
    t.price,
    t.notional_amount,
    t.commission,
    t.currency,
    t.settlement_days,
    i.instrument_name,
    i.instrument_type,
    i.sector,
    i.liquidity_class,
    c.counterparty_name,
    c.counterparty_type,
    c.credit_rating,
    p.portfolio_name,
    p.strategy,
    p.fund_id,
    p.manager_name,
    d.fiscal_quarter,
    d.fiscal_year,
    d.is_month_end,
        CASE
            WHEN ((EXTRACT(month FROM t.trade_date) >= (1)::numeric) AND (EXTRACT(month FROM t.trade_date) <= (3)::numeric)) THEN 'Q3'::text
            WHEN ((EXTRACT(month FROM t.trade_date) >= (4)::numeric) AND (EXTRACT(month FROM t.trade_date) <= (6)::numeric)) THEN 'Q4'::text
            WHEN ((EXTRACT(month FROM t.trade_date) >= (7)::numeric) AND (EXTRACT(month FROM t.trade_date) <= (9)::numeric)) THEN 'Q1'::text
            WHEN ((EXTRACT(month FROM t.trade_date) >= (10)::numeric) AND (EXTRACT(month FROM t.trade_date) <= (12)::numeric)) THEN 'Q2'::text
            ELSE NULL::text
        END AS computed_fiscal_quarter,
    sum(t.notional_amount) OVER (PARTITION BY t.portfolio_id ORDER BY t.trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_notional,
    count(*) OVER (PARTITION BY t.portfolio_id, (date_trunc('month'::text, (t.trade_date)::timestamp with time zone))) AS monthly_trade_count,
    sum(t.commission) OVER (PARTITION BY t.portfolio_id, (date_trunc('month'::text, (t.trade_date)::timestamp with time zone))) AS monthly_commissions
   FROM ((((stg_trades t
     JOIN stg_instruments i ON ((t.instrument_id = i.instrument_id)))
     JOIN stg_counterparties c ON ((t.counterparty_id = c.counterparty_id)))
     JOIN stg_portfolios p ON ((t.portfolio_id = (p.portfolio_id)::text)))
     JOIN stg_dates d ON ((t.trade_date = d.date_day)));
```

</details>

---

## `public`.`int_benchmark_returns`

**Type:** `VIEW` | **Rows:** 35  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `benchmark_id` | `TEXT` | YES |  |  |
| 2 | `benchmark_name` | `TEXT` | YES |  |  |
| 3 | `benchmark_date` | `DATE` | YES |  |  |
| 4 | `return_daily` | `NUMERIC(18,8)` | YES |  |  |
| 5 | `return_mtd` | `NUMERIC(18,8)` | YES |  |  |
| 6 | `return_ytd` | `NUMERIC(18,8)` | YES |  |  |
| 7 | `benchmark_level` | `NUMERIC(18,4)` | YES |  |  |
| 8 | `benchmark_fiscal_quarter` | `TEXT` | YES |  |  |
| 9 | `fiscal_year` | `TEXT` | YES |  |  |
| 10 | `is_quarter_end` | `BOOLEAN` | YES |  |  |
| 11 | `is_year_end` | `BOOLEAN` | YES |  |  |
| 12 | `rolling_3m_return` | `NUMERIC` | YES |  |  |
| 13 | `rolling_6m_return` | `NUMERIC` | YES |  |  |
| 14 | `rolling_12m_return` | `NUMERIC` | YES |  |  |
| 15 | `rolling_12m_volatility` | `NUMERIC` | YES |  |  |
| 16 | `rolling_12m_avg_return` | `NUMERIC` | YES |  |  |
| 17 | `prev_benchmark_level` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH benchmark_base AS (
         SELECT b.benchmark_id,
            b.benchmark_name,
            b.benchmark_date,
            b.return_daily,
            b.return_mtd,
            b.return_ytd,
            b.benchmark_level,
            b.benchmark_fiscal_quarter,
            d.fiscal_year,
            d.is_quarter_end,
            d.is_year_end
           FROM (stg_benchmarks b
             JOIN stg_dates d ON ((b.benchmark_date = d.date_day)))
        ), benchmark_with_rolling AS (
         SELECT bb.benchmark_id,
            bb.benchmark_name,
            bb.benchmark_date,
            bb.return_daily,
            bb.return_mtd,
            bb.return_ytd,
            bb.benchmark_level,
            bb.benchmark_fiscal_quarter,
            bb.fiscal_year,
            bb.is_quarter_end,
            bb.is_year_end,
            (exp(sum(ln(((1)::numeric + bb.return_mtd))) OVER (PARTITION BY bb.benchmark_id ORDER BY bb.benchmark_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)) - (1)::numeric) AS rolling_3m_return,
            (exp(sum(ln(((1)::numeric + bb.return_mtd))) OVER (PARTITION BY bb.benchmark_id ORDER BY bb.benchmark_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)) - (1)::numeric) AS rolling_6m_return,
            (exp(sum(ln(((1)::numeric + bb.return_mtd))) OVER (PARTITION BY bb.benchmark_id ORDER BY bb.benchmark_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)) - (1)::numeric) AS rolling_12m_return,
            stddev(bb.return_mtd) OVER (PARTITION BY bb.benchmark_id ORDER BY bb.benchmark_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12m_volatility,
            avg(bb.return_mtd) OVER (PARTITION BY bb.benchmark_id ORDER BY bb.benchmark_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS rolling_12m_avg_return,
            lag(bb.benchmark_level) OVER (PARTITION BY bb.benchmark_id ORDER BY bb.benchmark_date) AS prev_benchmark_level
           FROM benchmark_base bb
        )
 SELECT benchmark_id,
    benchmark_name,
    benchmark_date,
    return_daily,
    return_mtd,
    return_ytd,
    benchmark_level,
    benchmark_fiscal_quarter,
    fiscal_year,
    is_quarter_end,
    is_year_end,
    rolling_3m_return,
    rolling_6m_return,
    rolling_12m_return,
    rolling_12m_volatility,
    rolling_12m_avg_return,
    prev_benchmark_level
   FROM benchmark_with_rolling;
```

</details>

---

## `public`.`int_cashflow_enriched`

**Type:** `VIEW` | **Rows:** 25  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `cashflow_id` | `TEXT` | YES |  |  |
| 2 | `fund_id` | `TEXT` | YES |  |  |
| 3 | `fund_name` | `TEXT` | YES |  |  |
| 4 | `fund_type` | `TEXT` | YES |  |  |
| 5 | `portfolio_id` | `TEXT` | YES |  |  |
| 6 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 7 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 8 | `manager_name` | `VARCHAR(100)` | YES |  |  |
| 9 | `cashflow_date` | `DATE` | YES |  |  |
| 10 | `cashflow_type` | `TEXT` | YES |  |  |
| 11 | `amount` | `NUMERIC(18,2)` | YES |  |  |
| 12 | `currency` | `TEXT` | YES |  |  |
| 13 | `description` | `TEXT` | YES |  |  |
| 14 | `investor_id` | `TEXT` | YES |  |  |
| 15 | `cashflow_fiscal_quarter` | `TEXT` | YES |  |  |
| 16 | `cashflow_year` | `NUMERIC` | YES |  |  |
| 17 | `committed_capital` | `NUMERIC(18,2)` | YES |  |  |
| 18 | `management_fee_rate` | `NUMERIC(8,4)` | YES |  |  |
| 19 | `carry_rate` | `NUMERIC(8,4)` | YES |  |  |
| 20 | `hurdle_rate` | `NUMERIC(8,4)` | YES |  |  |
| 21 | `cumulative_called` | `NUMERIC` | YES |  |  |
| 22 | `cumulative_distributed` | `NUMERIC` | YES |  |  |
| 23 | `cumulative_net_cashflow` | `NUMERIC` | YES |  |  |
| 24 | `pct_called` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH cashflow_with_details AS (
         SELECT sub.cashflow_id,
            sub.fund_id,
            sub.portfolio_id,
            sub.cashflow_date,
            sub.cashflow_type,
            sub.amount,
            sub.currency,
            sub.description,
            sub.investor_id,
            sub.cashflow_fiscal_quarter,
            sub.cashflow_year,
            sub.portfolio_name,
            sub.strategy,
            sub.manager_name,
            sub.fund_name,
            sub.fund_type,
            sub.committed_capital,
            sub.management_fee_rate,
            sub.carry_rate,
            sub.hurdle_rate,
            sub.rn
           FROM ( SELECT cf.cashflow_id,
                    cf.fund_id,
                    cf.portfolio_id,
                    cf.cashflow_date,
                    cf.cashflow_type,
                    cf.amount,
                    cf.currency,
                    cf.description,
                    cf.investor_id,
                    cf.cashflow_fiscal_quarter,
                    cf.cashflow_year,
                    p.portfolio_name,
                    p.strategy,
                    p.manager_name,
                    fs.fund_name,
                    fs.fund_type,
                    fs.committed_capital,
                    fs.management_fee_rate,
                    fs.carry_rate,
                    fs.hurdle_rate,
                    row_number() OVER (PARTITION BY cf.cashflow_id ORDER BY cf.cashflow_date DESC) AS rn
                   FROM ((stg_cashflows cf
                     JOIN stg_portfolios p ON ((cf.portfolio_id = (p.portfolio_id)::text)))
                     JOIN stg_fund_structures fs ON ((cf.fund_id = fs.fund_id)))) sub
          WHERE (sub.rn = 1)
        ), cashflow_cumulative AS (
         SELECT cashflow_with_details.cashflow_id,
            cashflow_with_details.fund_id,
            cashflow_with_details.portfolio_id,
            cashflow_with_details.cashflow_date,
            cashflow_with_details.cashflow_type,
            cashflow_with_details.amount,
            cashflow_with_details.currency,
            cashflow_with_details.description,
            cashflow_with_details.investor_id,
            cashflow_with_details.cashflow_fiscal_quarter,
            cashflow_with_details.cashflow_year,
            cashflow_with_details.portfolio_name,
            cashflow_with_details.strategy,
            cashflow_with_details.manager_name,
            cashflow_with_details.fund_name,
            cashflow_with_details.fund_type,
            cashflow_with_details.committed_capital,
            cashflow_with_details.management_fee_rate,
            cashflow_with_details.carry_rate,
            cashflow_with_details.hurdle_rate,
            cashflow_with_details.rn,
            sum(
                CASE
                    WHEN (cashflow_with_details.cashflow_type = 'capital_call'::text) THEN cashflow_with_details.amount
                    ELSE (0)::numeric
                END) OVER (PARTITION BY cashflow_with_details.fund_id, cashflow_with_details.portfolio_id ORDER BY cashflow_with_details.cashflow_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_called,
            sum(
                CASE
                    WHEN (cashflow_with_details.cashflow_type = 'distribution'::text) THEN cashflow_with_details.amount
                    ELSE (0)::numeric
                END) OVER (PARTITION BY cashflow_with_details.fund_id, cashflow_with_details.portfolio_id ORDER BY cashflow_with_details.cashflow_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_distributed,
            sum(cashflow_with_details.amount) OVER (PARTITION BY cashflow_with_details.fund_id, cashflow_with_details.portfolio_id ORDER BY cashflow_with_details.cashflow_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_net_cashflow
           FROM cashflow_with_details
        )
 SELECT cashflow_id,
    fund_id,
    fund_name,
    fund_type,
    portfolio_id,
    portfolio_name,
    strategy,
    manager_name,
    cashflow_date,
    cashflow_type,
    amount,
    currency,
    description,
    investor_id,
    cashflow_fiscal_quarter,
    cashflow_year,
    committed_capital,
    management_fee_rate,
    carry_rate,
    hurdle_rate,
    cumulative_called,
    cumulative_distributed,
    cumulative_net_cashflow,
        CASE
            WHEN (committed_capital > (0)::numeric) THEN (cumulative_called / committed_capital)
            ELSE (0)::numeric
        END AS pct_called
   FROM cashflow_cumulative;
```

</details>

---

## `public`.`int_daily_positions`

**Type:** `VIEW` | **Rows:** 30  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `position_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `instrument_id` | `TEXT` | YES |  |  |
| 4 | `position_date` | `DATE` | YES |  |  |
| 5 | `quantity` | `NUMERIC(18,4)` | YES |  |  |
| 6 | `market_value` | `NUMERIC(18,2)` | YES |  |  |
| 7 | `cost_basis` | `NUMERIC(18,2)` | YES |  |  |
| 8 | `unrealized_gain_loss` | `NUMERIC` | YES |  |  |
| 9 | `unrealized_return_pct` | `NUMERIC` | YES |  |  |
| 10 | `currency` | `TEXT` | YES |  |  |
| 11 | `instrument_name` | `TEXT` | YES |  |  |
| 12 | `instrument_type` | `TEXT` | YES |  |  |
| 13 | `sector` | `TEXT` | YES |  |  |
| 14 | `liquidity_class` | `TEXT` | YES |  |  |
| 15 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 16 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 17 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 18 | `is_business_day` | `BOOLEAN` | YES |  |  |
| 19 | `is_month_end` | `BOOLEAN` | YES |  |  |
| 20 | `is_quarter_end` | `BOOLEAN` | YES |  |  |
| 21 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 22 | `fiscal_year` | `TEXT` | YES |  |  |
| 23 | `prev_market_value` | `NUMERIC` | YES |  |  |
| 24 | `daily_pnl` | `NUMERIC` | YES |  |  |
| 25 | `portfolio_total_mv` | `NUMERIC` | YES |  |  |
| 26 | `weight_in_portfolio` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH position_base AS (
         SELECT pos.position_id,
            pos.portfolio_id,
            pos.instrument_id,
            pos.position_date,
            pos.quantity,
            pos.market_value,
            pos.cost_basis,
            pos.unrealized_gain_loss,
            pos.unrealized_return_pct,
            pos.currency
           FROM stg_positions pos
        ), position_with_instruments AS (
         SELECT pb.position_id,
            pb.portfolio_id,
            pb.instrument_id,
            pb.position_date,
            pb.quantity,
            pb.market_value,
            pb.cost_basis,
            pb.unrealized_gain_loss,
            pb.unrealized_return_pct,
            pb.currency,
            i.instrument_name,
            i.instrument_type,
            i.sector,
            i.liquidity_class
           FROM (position_base pb
             JOIN stg_instruments i ON ((pb.instrument_id = i.instrument_id)))
        ), position_with_portfolio AS (
         SELECT pwi.position_id,
            pwi.portfolio_id,
            pwi.instrument_id,
            pwi.position_date,
            pwi.quantity,
            pwi.market_value,
            pwi.cost_basis,
            pwi.unrealized_gain_loss,
            pwi.unrealized_return_pct,
            pwi.currency,
            pwi.instrument_name,
            pwi.instrument_type,
            pwi.sector,
            pwi.liquidity_class,
            p.portfolio_name,
            p.strategy,
            p.fund_id
           FROM (position_with_instruments pwi
             JOIN stg_portfolios p ON ((pwi.portfolio_id = (p.portfolio_id)::text)))
        ), position_with_dates AS (
         SELECT pwp.position_id,
            pwp.portfolio_id,
            pwp.instrument_id,
            pwp.position_date,
            pwp.quantity,
            pwp.market_value,
            pwp.cost_basis,
            pwp.unrealized_gain_loss,
            pwp.unrealized_return_pct,
            pwp.currency,
            pwp.instrument_name,
            pwp.instrument_type,
            pwp.sector,
            pwp.liquidity_class,
            pwp.portfolio_name,
            pwp.strategy,
            pwp.fund_id,
            d.is_business_day,
            d.is_month_end,
            d.is_quarter_end,
            d.fiscal_quarter,
            d.fiscal_year
           FROM (position_with_portfolio pwp
             JOIN stg_dates d ON ((pwp.position_date = d.date_day)))
        ), position_with_analytics AS (
         SELECT pwd.position_id,
            pwd.portfolio_id,
            pwd.instrument_id,
            pwd.position_date,
            pwd.quantity,
            pwd.market_value,
            pwd.cost_basis,
            pwd.unrealized_gain_loss,
            pwd.unrealized_return_pct,
            pwd.currency,
            pwd.instrument_name,
            pwd.instrument_type,
            pwd.sector,
            pwd.liquidity_class,
            pwd.portfolio_name,
            pwd.strategy,
            pwd.fund_id,
            pwd.is_business_day,
            pwd.is_month_end,
            pwd.is_quarter_end,
            pwd.fiscal_quarter,
            pwd.fiscal_year,
            lag(pwd.market_value) OVER (PARTITION BY pwd.portfolio_id, pwd.instrument_id ORDER BY pwd.position_date) AS prev_market_value,
            (pwd.market_value - COALESCE(lag(pwd.market_value) OVER (PARTITION BY pwd.portfolio_id, pwd.instrument_id ORDER BY pwd.position_date), pwd.cost_basis)) AS daily_pnl,
            sum(pwd.market_value) OVER (PARTITION BY pwd.portfolio_id, pwd.position_date) AS portfolio_total_mv,
                CASE
                    WHEN (sum(pwd.market_value) OVER (PARTITION BY pwd.portfolio_id, pwd.position_date) <> (0)::numeric) THEN (pwd.market_value / sum(pwd.market_value) OVER (PARTITION BY pwd.portfolio_id, pwd.position_date))
                    ELSE (0)::numeric
                END AS weight_in_portfolio
           FROM position_with_dates pwd
        )
 SELECT position_id,
    portfolio_id,
    instrument_id,
    position_date,
    quantity,
    market_value,
    cost_basis,
    unrealized_gain_loss,
    unrealized_return_pct,
    currency,
    instrument_name,
    instrument_type,
    sector,
    liquidity_class,
    portfolio_name,
    strategy,
    fund_id,
    is_business_day,
    is_month_end,
    is_quarter_end,
    fiscal_quarter,
    fiscal_year,
    prev_market_value,
    daily_pnl,
    portfolio_total_mv,
    weight_in_portfolio
   FROM position_with_analytics;
```

</details>

---

## `public`.`int_fund_nav`

**Type:** `VIEW` | **Rows:** 12  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 2 | `fund_name` | `TEXT` | YES |  |  |
| 3 | `fund_type` | `TEXT` | YES |  |  |
| 4 | `committed_capital` | `NUMERIC(18,2)` | YES |  |  |
| 5 | `management_fee_rate` | `NUMERIC(8,4)` | YES |  |  |
| 6 | `valuation_date` | `DATE` | YES |  |  |
| 7 | `fund_nav` | `NUMERIC` | YES |  |  |
| 8 | `fund_gav` | `NUMERIC` | YES |  |  |
| 9 | `fund_total_liabilities` | `NUMERIC` | YES |  |  |
| 10 | `fund_unrealized_pnl` | `NUMERIC` | YES |  |  |
| 11 | `fund_realized_pnl` | `NUMERIC` | YES |  |  |
| 12 | `num_portfolios` | `BIGINT` | YES |  |  |
| 13 | `prev_fund_nav` | `NUMERIC` | YES |  |  |
| 14 | `fund_nav_return` | `NUMERIC` | YES |  |  |
| 15 | `tvpi_gross` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH portfolio_valuations AS (
         SELECT v.portfolio_id,
            v.valuation_date,
            v.net_asset_value,
            v.gross_asset_value,
            v.total_liabilities,
            v.unrealized_pnl,
            v.realized_pnl,
            v.valuation_method,
            p.fund_id,
            p.portfolio_name,
            p.strategy,
            fs.fund_name,
            fs.fund_type,
            fs.committed_capital,
            fs.management_fee_rate
           FROM ((stg_valuations v
             JOIN stg_portfolios p ON ((v.portfolio_id = (p.portfolio_id)::text)))
             JOIN stg_fund_structures fs ON (((p.fund_id)::text = fs.fund_id)))
        ), fund_level_nav AS (
         SELECT portfolio_valuations.fund_id,
            portfolio_valuations.fund_name,
            portfolio_valuations.fund_type,
            portfolio_valuations.committed_capital,
            portfolio_valuations.management_fee_rate,
            portfolio_valuations.valuation_date,
            sum(portfolio_valuations.net_asset_value) AS fund_nav,
            sum(portfolio_valuations.gross_asset_value) AS fund_gav,
            sum(portfolio_valuations.total_liabilities) AS fund_total_liabilities,
            sum(portfolio_valuations.unrealized_pnl) AS fund_unrealized_pnl,
            sum(portfolio_valuations.realized_pnl) AS fund_realized_pnl,
            count(DISTINCT portfolio_valuations.portfolio_id) AS num_portfolios
           FROM portfolio_valuations
          GROUP BY portfolio_valuations.fund_id, portfolio_valuations.fund_name, portfolio_valuations.fund_type, portfolio_valuations.committed_capital, portfolio_valuations.management_fee_rate, portfolio_valuations.valuation_date
        ), fund_nav_with_changes AS (
         SELECT fn.fund_id,
            fn.fund_name,
            fn.fund_type,
            fn.committed_capital,
            fn.management_fee_rate,
            fn.valuation_date,
            fn.fund_nav,
            fn.fund_gav,
            fn.fund_total_liabilities,
            fn.fund_unrealized_pnl,
            fn.fund_realized_pnl,
            fn.num_portfolios,
            lag(fn.fund_nav) OVER (PARTITION BY fn.fund_id ORDER BY fn.valuation_date) AS prev_fund_nav,
                CASE
                    WHEN ((lag(fn.fund_nav) OVER (PARTITION BY fn.fund_id ORDER BY fn.valuation_date) IS NOT NULL) AND (lag(fn.fund_nav) OVER (PARTITION BY fn.fund_id ORDER BY fn.valuation_date) <> (0)::numeric)) THEN ((fn.fund_nav - lag(fn.fund_nav) OVER (PARTITION BY fn.fund_id ORDER BY fn.valuation_date)) / lag(fn.fund_nav) OVER (PARTITION BY fn.fund_id ORDER BY fn.valuation_date))
                    ELSE NULL::numeric
                END AS fund_nav_return,
            (fn.fund_nav / NULLIF(fn.committed_capital, (0)::numeric)) AS tvpi_gross
           FROM fund_level_nav fn
        )
 SELECT fund_id,
    fund_name,
    fund_type,
    committed_capital,
    management_fee_rate,
    valuation_date,
    fund_nav,
    fund_gav,
    fund_total_liabilities,
    fund_unrealized_pnl,
    fund_realized_pnl,
    num_portfolios,
    prev_fund_nav,
    fund_nav_return,
    tvpi_gross
   FROM fund_nav_with_changes;
```

</details>

---

## `public`.`int_irr_calculations`

**Type:** `VIEW` | **Rows:** 5  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `fund_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `first_cashflow_date` | `DATE` | YES |  |  |
| 4 | `last_cashflow_date` | `DATE` | YES |  |  |
| 5 | `investment_days` | `INTEGER` | YES |  |  |
| 6 | `investment_years` | `NUMERIC` | YES |  |  |
| 7 | `total_invested` | `NUMERIC` | YES |  |  |
| 8 | `total_distributed` | `NUMERIC` | YES |  |  |
| 9 | `terminal_value` | `NUMERIC` | YES |  |  |
| 10 | `net_cashflow` | `NUMERIC` | YES |  |  |
| 11 | `tvpi` | `NUMERIC` | YES |  |  |
| 12 | `dpi` | `NUMERIC` | YES |  |  |
| 13 | `rvpi` | `NUMERIC` | YES |  |  |
| 14 | `approx_irr` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH cashflow_series AS (
         SELECT cf.fund_id,
            cf.portfolio_id,
            cf.cashflow_date,
            cf.cashflow_type,
                CASE
                    WHEN (cf.cashflow_type = 'capital_call'::text) THEN (('-1'::integer)::numeric * abs(cf.amount))
                    WHEN (cf.cashflow_type = 'distribution'::text) THEN abs(cf.amount)
                    WHEN (cf.cashflow_type = 'management_fee'::text) THEN (('-1'::integer)::numeric * abs(cf.amount))
                    ELSE cf.amount
                END AS signed_amount
           FROM stg_cashflows cf
        ), terminal_values AS (
         SELECT v.portfolio_id,
            p.fund_id,
            v.valuation_date AS cashflow_date,
            'terminal_value'::text AS cashflow_type,
            v.net_asset_value AS signed_amount
           FROM (stg_valuations v
             JOIN stg_portfolios p ON ((v.portfolio_id = (p.portfolio_id)::text)))
          WHERE (v.valuation_date = ( SELECT max(v2.valuation_date) AS max
                   FROM stg_valuations v2
                  WHERE (v2.portfolio_id = v.portfolio_id)))
        ), all_cashflows AS (
         SELECT cashflow_series.fund_id,
            cashflow_series.portfolio_id,
            cashflow_series.cashflow_date,
            cashflow_series.cashflow_type,
            cashflow_series.signed_amount
           FROM cashflow_series
        UNION ALL
         SELECT terminal_values.fund_id,
            terminal_values.portfolio_id,
            terminal_values.cashflow_date,
            terminal_values.cashflow_type,
            terminal_values.signed_amount
           FROM terminal_values
        ), cashflow_with_timing AS (
         SELECT ac.fund_id,
            ac.portfolio_id,
            ac.cashflow_date,
            ac.cashflow_type,
            ac.signed_amount,
            min(ac.cashflow_date) OVER (PARTITION BY ac.fund_id, ac.portfolio_id) AS first_cf_date,
            (((ac.cashflow_date - min(ac.cashflow_date) OVER (PARTITION BY ac.fund_id, ac.portfolio_id)))::numeric / 365.25) AS year_fraction
           FROM all_cashflows ac
        ), portfolio_multiples AS (
         SELECT cashflow_with_timing.fund_id,
            cashflow_with_timing.portfolio_id,
            min(cashflow_with_timing.cashflow_date) AS first_cashflow_date,
            max(cashflow_with_timing.cashflow_date) AS last_cashflow_date,
            (max(cashflow_with_timing.cashflow_date) - min(cashflow_with_timing.cashflow_date)) AS investment_days,
            (((max(cashflow_with_timing.cashflow_date) - min(cashflow_with_timing.cashflow_date)))::numeric / 365.25) AS investment_years,
            sum(
                CASE
                    WHEN (cashflow_with_timing.cashflow_type = 'capital_call'::text) THEN abs(cashflow_with_timing.signed_amount)
                    ELSE (0)::numeric
                END) AS total_invested,
            sum(
                CASE
                    WHEN (cashflow_with_timing.cashflow_type = 'distribution'::text) THEN cashflow_with_timing.signed_amount
                    ELSE (0)::numeric
                END) AS total_distributed,
            sum(
                CASE
                    WHEN (cashflow_with_timing.cashflow_type = 'terminal_value'::text) THEN cashflow_with_timing.signed_amount
                    ELSE (0)::numeric
                END) AS terminal_value,
            sum(cashflow_with_timing.signed_amount) AS net_cashflow
           FROM cashflow_with_timing
          GROUP BY cashflow_with_timing.fund_id, cashflow_with_timing.portfolio_id
        ), irr_approximation AS (
         SELECT pm.fund_id,
            pm.portfolio_id,
            pm.first_cashflow_date,
            pm.last_cashflow_date,
            pm.investment_days,
            pm.investment_years,
            pm.total_invested,
            pm.total_distributed,
            pm.terminal_value,
            pm.net_cashflow,
                CASE
                    WHEN (pm.total_invested > (0)::numeric) THEN ((pm.total_distributed + pm.terminal_value) / pm.total_invested)
                    ELSE NULL::numeric
                END AS tvpi,
                CASE
                    WHEN (pm.total_invested > (0)::numeric) THEN (pm.total_distributed / pm.total_invested)
                    ELSE NULL::numeric
                END AS dpi,
                CASE
                    WHEN (pm.total_invested > (0)::numeric) THEN (pm.terminal_value / pm.total_invested)
                    ELSE NULL::numeric
                END AS rvpi,
                CASE
                    WHEN ((pm.total_invested > (0)::numeric) AND (pm.investment_years > (0)::numeric)) THEN (power(((pm.total_distributed + pm.terminal_value) / pm.total_invested), (1.0 / pm.investment_years)) - (1)::numeric)
                    ELSE NULL::numeric
                END AS approx_irr
           FROM portfolio_multiples pm
        )
 SELECT fund_id,
    portfolio_id,
    first_cashflow_date,
    last_cashflow_date,
    investment_days,
    investment_years,
    total_invested,
    total_distributed,
    terminal_value,
    net_cashflow,
    tvpi,
    dpi,
    rvpi,
    approx_irr
   FROM irr_approximation;
```

</details>

---

## `public`.`int_portfolio_attribution`

**Type:** `VIEW` | **Rows:** 22  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `portfolio_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 3 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 4 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 5 | `position_date` | `DATE` | YES |  |  |
| 6 | `sector` | `TEXT` | YES |  |  |
| 7 | `sector_mv` | `NUMERIC` | YES |  |  |
| 8 | `sector_ugl` | `NUMERIC` | YES |  |  |
| 9 | `num_instruments` | `BIGINT` | YES |  |  |
| 10 | `total_portfolio_mv` | `NUMERIC` | YES |  |  |
| 11 | `total_portfolio_ugl` | `NUMERIC` | YES |  |  |
| 12 | `sector_weight` | `NUMERIC` | YES |  |  |
| 13 | `sector_contribution` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH positions_enriched AS (
         SELECT pos.position_id,
            pos.portfolio_id,
            pos.instrument_id,
            pos.position_date,
            pos.quantity,
            pos.market_value,
            pos.cost_basis,
            pos.unrealized_gain_loss,
            i.instrument_name,
            i.instrument_type,
            i.sector,
            p.portfolio_name,
            p.strategy,
            p.fund_id
           FROM ((stg_positions pos
             JOIN stg_instruments i ON ((pos.instrument_id = i.instrument_id)))
             JOIN stg_portfolios p ON ((pos.portfolio_id = (p.portfolio_id)::text)))
        ), portfolio_totals AS (
         SELECT positions_enriched.portfolio_id,
            positions_enriched.position_date,
            sum(positions_enriched.market_value) AS total_portfolio_mv,
            sum(positions_enriched.unrealized_gain_loss) AS total_portfolio_ugl
           FROM positions_enriched
          GROUP BY positions_enriched.portfolio_id, positions_enriched.position_date
        ), sector_attribution AS (
         SELECT pe.portfolio_id,
            pe.portfolio_name,
            pe.strategy,
            pe.fund_id,
            pe.position_date,
            pe.sector,
            sum(pe.market_value) AS sector_mv,
            sum(pe.unrealized_gain_loss) AS sector_ugl,
            count(DISTINCT pe.instrument_id) AS num_instruments
           FROM positions_enriched pe
          GROUP BY pe.portfolio_id, pe.portfolio_name, pe.strategy, pe.fund_id, pe.position_date, pe.sector
        ), attribution_with_weights AS (
         SELECT sa.portfolio_id,
            sa.portfolio_name,
            sa.strategy,
            sa.fund_id,
            sa.position_date,
            sa.sector,
            sa.sector_mv,
            sa.sector_ugl,
            sa.num_instruments,
            pt.total_portfolio_mv,
            pt.total_portfolio_ugl,
                CASE
                    WHEN (pt.total_portfolio_mv <> (0)::numeric) THEN (sa.sector_mv / pt.total_portfolio_mv)
                    ELSE (0)::numeric
                END AS sector_weight,
                CASE
                    WHEN (pt.total_portfolio_ugl <> (0)::numeric) THEN (sa.sector_ugl / pt.total_portfolio_ugl)
                    ELSE (0)::numeric
                END AS sector_contribution
           FROM (sector_attribution sa
             JOIN portfolio_totals pt ON (((sa.portfolio_id = pt.portfolio_id) AND (sa.position_date = pt.position_date))))
        )
 SELECT portfolio_id,
    portfolio_name,
    strategy,
    fund_id,
    position_date,
    sector,
    sector_mv,
    sector_ugl,
    num_instruments,
    total_portfolio_mv,
    total_portfolio_ugl,
    sector_weight,
    sector_contribution
   FROM attribution_with_weights;
```

</details>

---

## `public`.`int_trade_enriched`

**Type:** `VIEW` | **Rows:** 40  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `trade_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 4 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 5 | `manager_name` | `VARCHAR(100)` | YES |  |  |
| 6 | `instrument_id` | `TEXT` | YES |  |  |
| 7 | `instrument_name` | `TEXT` | YES |  |  |
| 8 | `instrument_type` | `TEXT` | YES |  |  |
| 9 | `sector` | `TEXT` | YES |  |  |
| 10 | `liquidity_class` | `TEXT` | YES |  |  |
| 11 | `counterparty_id` | `TEXT` | YES |  |  |
| 12 | `counterparty_name` | `TEXT` | YES |  |  |
| 13 | `counterparty_type` | `TEXT` | YES |  |  |
| 14 | `credit_rating` | `TEXT` | YES |  |  |
| 15 | `trade_date` | `DATE` | YES |  |  |
| 16 | `settlement_date` | `DATE` | YES |  |  |
| 17 | `trade_type` | `TEXT` | YES |  |  |
| 18 | `quantity` | `NUMERIC(18,4)` | YES |  |  |
| 19 | `price` | `NUMERIC(18,6)` | YES |  |  |
| 20 | `notional_amount` | `NUMERIC(18,2)` | YES |  |  |
| 21 | `commission` | `NUMERIC(18,2)` | YES |  |  |
| 22 | `currency` | `TEXT` | YES |  |  |
| 23 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 24 | `settlement_days` | `INTEGER` | YES |  |  |
| 25 | `cumulative_notional` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH latest_trades AS (
         SELECT sub.trade_id,
            sub.portfolio_id,
            sub.instrument_id,
            sub.counterparty_id,
            sub.trade_date,
            sub.settlement_date,
            sub.trade_type,
            sub.quantity,
            sub.price,
            sub.notional_amount,
            sub.commission,
            sub.currency,
            sub.status,
            sub.trade_fiscal_quarter,
            sub.settlement_days,
            sub.instrument_name,
            sub.instrument_type,
            sub.sector,
            sub.liquidity_class,
            sub.counterparty_name,
            sub.counterparty_type,
            sub.credit_rating,
            sub.portfolio_name,
            sub.strategy,
            sub.manager_name,
            sub.fiscal_quarter,
            sub.rn
           FROM ( SELECT t.trade_id,
                    t.portfolio_id,
                    t.instrument_id,
                    t.counterparty_id,
                    t.trade_date,
                    t.settlement_date,
                    t.trade_type,
                    t.quantity,
                    t.price,
                    t.notional_amount,
                    t.commission,
                    t.currency,
                    t.status,
                    t.trade_fiscal_quarter,
                    t.settlement_days,
                    i.instrument_name,
                    i.instrument_type,
                    i.sector,
                    i.liquidity_class,
                    c.counterparty_name,
                    c.counterparty_type,
                    c.credit_rating,
                    p.portfolio_name,
                    p.strategy,
                    p.manager_name,
                        CASE
                            WHEN ((EXTRACT(month FROM t.trade_date) >= (1)::numeric) AND (EXTRACT(month FROM t.trade_date) <= (3)::numeric)) THEN 'Q3'::text
                            WHEN ((EXTRACT(month FROM t.trade_date) >= (4)::numeric) AND (EXTRACT(month FROM t.trade_date) <= (6)::numeric)) THEN 'Q4'::text
                            WHEN ((EXTRACT(month FROM t.trade_date) >= (7)::numeric) AND (EXTRACT(month FROM t.trade_date) <= (9)::numeric)) THEN 'Q1'::text
                            WHEN ((EXTRACT(month FROM t.trade_date) >= (10)::numeric) AND (EXTRACT(month FROM t.trade_date) <= (12)::numeric)) THEN 'Q2'::text
                            ELSE NULL::text
                        END AS fiscal_quarter,
                    row_number() OVER (PARTITION BY t.portfolio_id, t.instrument_id, t.trade_date ORDER BY t.trade_id DESC) AS rn
                   FROM (((stg_trades t
                     JOIN stg_instruments i ON ((t.instrument_id = i.instrument_id)))
                     JOIN stg_counterparties c ON ((t.counterparty_id = c.counterparty_id)))
                     JOIN stg_portfolios p ON ((t.portfolio_id = (p.portfolio_id)::text)))) sub
          WHERE (sub.rn = 1)
        )
 SELECT trade_id,
    portfolio_id,
    portfolio_name,
    strategy,
    manager_name,
    instrument_id,
    instrument_name,
    instrument_type,
    sector,
    liquidity_class,
    counterparty_id,
    counterparty_name,
    counterparty_type,
    credit_rating,
    trade_date,
    settlement_date,
    trade_type,
    quantity,
    price,
    notional_amount,
    commission,
    currency,
    fiscal_quarter,
    settlement_days,
    sum(notional_amount) OVER (PARTITION BY portfolio_id ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_notional
   FROM latest_trades;
```

</details>

---

## `public`.`int_valuation_enriched`

**Type:** `VIEW` | **Rows:** 21  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `valuation_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `valuation_date` | `DATE` | YES |  |  |
| 4 | `gross_asset_value` | `NUMERIC(18,2)` | YES |  |  |
| 5 | `net_asset_value` | `NUMERIC(18,2)` | YES |  |  |
| 6 | `total_liabilities` | `NUMERIC(18,2)` | YES |  |  |
| 7 | `unrealized_pnl` | `NUMERIC(18,2)` | YES |  |  |
| 8 | `realized_pnl` | `NUMERIC(18,2)` | YES |  |  |
| 9 | `valuation_method` | `TEXT` | YES |  |  |
| 10 | `valuation_fiscal_quarter` | `TEXT` | YES |  |  |
| 11 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 12 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 13 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 14 | `manager_name` | `VARCHAR(100)` | YES |  |  |
| 15 | `rn` | `BIGINT` | YES |  |  |
| 16 | `prev_nav` | `NUMERIC` | YES |  |  |
| 17 | `nav_return_pct` | `NUMERIC` | YES |  |  |
| 18 | `days_between_valuations` | `INTEGER` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH valuation_ranked AS (
         SELECT sub.valuation_id,
            sub.portfolio_id,
            sub.valuation_date,
            sub.gross_asset_value,
            sub.net_asset_value,
            sub.total_liabilities,
            sub.unrealized_pnl,
            sub.realized_pnl,
            sub.valuation_method,
            sub.valuation_fiscal_quarter,
            sub.portfolio_name,
            sub.strategy,
            sub.fund_id,
            sub.manager_name,
            sub.rn
           FROM ( SELECT v.valuation_id,
                    v.portfolio_id,
                    v.valuation_date,
                    v.gross_asset_value,
                    v.net_asset_value,
                    v.total_liabilities,
                    v.unrealized_pnl,
                    v.realized_pnl,
                    v.valuation_method,
                    v.valuation_fiscal_quarter,
                    p.portfolio_name,
                    p.strategy,
                    p.fund_id,
                    p.manager_name,
                    row_number() OVER (PARTITION BY v.portfolio_id, v.valuation_date ORDER BY v.valuation_id DESC) AS rn
                   FROM (stg_valuations v
                     JOIN stg_portfolios p ON ((v.portfolio_id = (p.portfolio_id)::text)))) sub
          WHERE (sub.rn = 1)
        ), valuation_with_changes AS (
         SELECT vr.valuation_id,
            vr.portfolio_id,
            vr.valuation_date,
            vr.gross_asset_value,
            vr.net_asset_value,
            vr.total_liabilities,
            vr.unrealized_pnl,
            vr.realized_pnl,
            vr.valuation_method,
            vr.valuation_fiscal_quarter,
            vr.portfolio_name,
            vr.strategy,
            vr.fund_id,
            vr.manager_name,
            vr.rn,
            lag(vr.net_asset_value) OVER (PARTITION BY vr.portfolio_id ORDER BY vr.valuation_date) AS prev_nav,
                CASE
                    WHEN ((lag(vr.net_asset_value) OVER (PARTITION BY vr.portfolio_id ORDER BY vr.valuation_date) IS NOT NULL) AND (lag(vr.net_asset_value) OVER (PARTITION BY vr.portfolio_id ORDER BY vr.valuation_date) <> (0)::numeric)) THEN ((vr.net_asset_value - lag(vr.net_asset_value) OVER (PARTITION BY vr.portfolio_id ORDER BY vr.valuation_date)) / lag(vr.net_asset_value) OVER (PARTITION BY vr.portfolio_id ORDER BY vr.valuation_date))
                    ELSE NULL::numeric
                END AS nav_return_pct,
            (vr.valuation_date - lag(vr.valuation_date) OVER (PARTITION BY vr.portfolio_id ORDER BY vr.valuation_date)) AS days_between_valuations
           FROM valuation_ranked vr
        )
 SELECT valuation_id,
    portfolio_id,
    valuation_date,
    gross_asset_value,
    net_asset_value,
    total_liabilities,
    unrealized_pnl,
    realized_pnl,
    valuation_method,
    valuation_fiscal_quarter,
    portfolio_name,
    strategy,
    fund_id,
    manager_name,
    rn,
    prev_nav,
    nav_return_pct,
    days_between_valuations
   FROM valuation_with_changes;
```

</details>

---

## `public`.`report_daily_pnl`

**Type:** `VIEW` | **Rows:** 12  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `portfolio_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 3 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 4 | `pnl_date` | `DATE` | YES |  |  |
| 5 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 6 | `position_daily_pnl` | `NUMERIC` | YES |  |  |
| 7 | `cumulative_pnl` | `NUMERIC` | YES |  |  |
| 8 | `total_market_value` | `NUMERIC` | YES |  |  |
| 9 | `num_positions` | `BIGINT` | YES |  |  |
| 10 | `total_traded_notional` | `NUMERIC` | YES |  |  |
| 11 | `total_commissions` | `NUMERIC` | YES |  |  |
| 12 | `num_trades` | `BIGINT` | YES |  |  |
| 13 | `benchmark_name` | `TEXT` | YES |  |  |
| 14 | `benchmark_return_mtd` | `NUMERIC(18,8)` | YES |  |  |
| 15 | `benchmark_return_ytd` | `NUMERIC(18,8)` | YES |  |  |
| 16 | `daily_return_pct` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH latest_pnl AS (
         SELECT sub.portfolio_id,
            sub.portfolio_name,
            sub.strategy,
            sub.fund_id,
            sub.pnl_date,
            sub.fiscal_quarter,
            sub.fiscal_year,
            sub.position_daily_pnl,
            sub.total_market_value,
            sub.num_positions,
            sub.total_traded_notional,
            sub.total_commissions,
            sub.num_trades,
            sub.cumulative_pnl,
            sub.rn
           FROM ( SELECT fpnl.portfolio_id,
                    fpnl.portfolio_name,
                    fpnl.strategy,
                    fpnl.fund_id,
                    fpnl.pnl_date,
                    fpnl.fiscal_quarter,
                    fpnl.fiscal_year,
                    fpnl.position_daily_pnl,
                    fpnl.total_market_value,
                    fpnl.num_positions,
                    fpnl.total_traded_notional,
                    fpnl.total_commissions,
                    fpnl.num_trades,
                    fpnl.cumulative_pnl,
                    row_number() OVER (PARTITION BY fpnl.portfolio_id ORDER BY fpnl.pnl_date DESC) AS rn
                   FROM fact_portfolio_pnl fpnl) sub
          WHERE (sub.rn <= 5)
        ), pnl_with_benchmark AS (
         SELECT lp.portfolio_id,
            lp.portfolio_name,
            lp.strategy,
            lp.fund_id,
            lp.pnl_date,
            lp.fiscal_quarter,
            lp.fiscal_year,
            lp.position_daily_pnl,
            lp.total_market_value,
            lp.num_positions,
            lp.total_traded_notional,
            lp.total_commissions,
            lp.num_trades,
            lp.cumulative_pnl,
            lp.rn,
            bm.return_mtd AS benchmark_return_mtd,
            bm.return_ytd AS benchmark_return_ytd,
            bm.benchmark_name
           FROM (latest_pnl lp
             LEFT JOIN stg_benchmarks bm ON (((lp.pnl_date = bm.benchmark_date) AND (bm.benchmark_id = 'BM_SP500'::text))))
        )
 SELECT portfolio_id,
    portfolio_name,
    strategy,
    pnl_date,
    fiscal_quarter,
    position_daily_pnl,
    cumulative_pnl,
    total_market_value,
    num_positions,
    total_traded_notional,
    total_commissions,
    num_trades,
    benchmark_name,
    benchmark_return_mtd,
    benchmark_return_ytd,
        CASE
            WHEN (total_market_value <> (0)::numeric) THEN (position_daily_pnl / total_market_value)
            ELSE (0)::numeric
        END AS daily_return_pct
   FROM pnl_with_benchmark
  ORDER BY portfolio_id, pnl_date DESC;
```

</details>

---

## `public`.`report_ic_dashboard`

**Type:** `VIEW` | **Rows:** 5  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 2 | `fund_name` | `TEXT` | YES |  |  |
| 3 | `fund_type` | `TEXT` | YES |  |  |
| 4 | `vintage_year` | `INTEGER` | YES |  |  |
| 5 | `committed_capital` | `NUMERIC(18,2)` | YES |  |  |
| 6 | `latest_valuation_date` | `DATE` | YES |  |  |
| 7 | `fund_nav` | `NUMERIC` | YES |  |  |
| 8 | `fund_gav` | `NUMERIC` | YES |  |  |
| 9 | `latest_nav_return` | `NUMERIC` | YES |  |  |
| 10 | `tvpi_gross` | `NUMERIC` | YES |  |  |
| 11 | `fund_approx_irr` | `NUMERIC` | YES |  |  |
| 12 | `fund_tvpi` | `NUMERIC` | YES |  |  |
| 13 | `fund_dpi` | `NUMERIC` | YES |  |  |
| 14 | `num_portfolios` | `BIGINT` | YES |  |  |
| 15 | `fund_total_invested` | `NUMERIC` | YES |  |  |
| 16 | `fund_total_distributed` | `NUMERIC` | YES |  |  |
| 17 | `management_fee_rate` | `NUMERIC(8,4)` | YES |  |  |
| 18 | `carry_rate` | `NUMERIC(8,4)` | YES |  |  |
| 19 | `hurdle_rate` | `NUMERIC(8,4)` | YES |  |  |
| 20 | `portfolio_id` | `VARCHAR(10)` | YES |  |  |
| 21 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 22 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 23 | `portfolio_mv` | `NUMERIC` | YES |  |  |
| 24 | `portfolio_unrealized_pnl` | `NUMERIC` | YES |  |  |
| 25 | `portfolio_return_pct` | `NUMERIC` | YES |  |  |
| 26 | `portfolio_num_positions` | `BIGINT` | YES |  |  |
| 27 | `reporting_fiscal_quarter` | `TEXT` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH latest_fund_performance AS (
         SELECT sub.fund_id,
            sub.fund_name,
            sub.fund_type,
            sub.committed_capital,
            sub.valuation_date,
            sub.fund_nav,
            sub.fund_gav,
            sub.fund_total_liabilities,
            sub.fund_unrealized_pnl,
            sub.fund_realized_pnl,
            sub.fund_nav_return,
            sub.tvpi_gross,
            sub.num_portfolios,
            sub.period_calls,
            sub.period_distributions,
            sub.period_fees,
            sub.period_net_cashflow,
            sub.fund_total_invested,
            sub.fund_total_distributed,
            sub.fund_approx_irr,
            sub.fund_tvpi,
            sub.fund_dpi,
            sub.rn
           FROM ( SELECT fp.fund_id,
                    fp.fund_name,
                    fp.fund_type,
                    fp.committed_capital,
                    fp.valuation_date,
                    fp.fund_nav,
                    fp.fund_gav,
                    fp.fund_total_liabilities,
                    fp.fund_unrealized_pnl,
                    fp.fund_realized_pnl,
                    fp.fund_nav_return,
                    fp.tvpi_gross,
                    fp.num_portfolios,
                    fp.period_calls,
                    fp.period_distributions,
                    fp.period_fees,
                    fp.period_net_cashflow,
                    fp.fund_total_invested,
                    fp.fund_total_distributed,
                    fp.fund_approx_irr,
                    fp.fund_tvpi,
                    fp.fund_dpi,
                    row_number() OVER (PARTITION BY fp.fund_id ORDER BY fp.valuation_date DESC) AS rn
                   FROM fact_fund_performance fp) sub
          WHERE (sub.rn = 1)
        ), latest_portfolio_summary AS (
         SELECT sub.portfolio_id,
            sub.portfolio_name,
            sub.strategy,
            sub.fund_id,
            sub.manager_name,
            sub.position_date,
            sub.num_positions,
            sub.total_market_value,
            sub.total_cost_basis,
            sub.total_unrealized_pnl,
            sub.portfolio_return_pct,
            sub.largest_position_mv,
            sub.smallest_position_mv,
            sub.rn
           FROM ( SELECT ps.portfolio_id,
                    ps.portfolio_name,
                    ps.strategy,
                    ps.fund_id,
                    ps.manager_name,
                    ps.position_date,
                    ps.num_positions,
                    ps.total_market_value,
                    ps.total_cost_basis,
                    ps.total_unrealized_pnl,
                    ps.portfolio_return_pct,
                    ps.largest_position_mv,
                    ps.smallest_position_mv,
                    row_number() OVER (PARTITION BY ps.portfolio_id ORDER BY ps.position_date DESC) AS rn
                   FROM fact_portfolio_summary ps) sub
          WHERE (sub.rn = 1)
        ), fund_overview AS (
         SELECT lfp.fund_id,
            lfp.fund_name,
            lfp.fund_type,
            lfp.committed_capital,
            lfp.valuation_date AS latest_valuation_date,
            lfp.fund_nav,
            lfp.fund_gav,
            lfp.fund_nav_return AS latest_nav_return,
            lfp.tvpi_gross,
            lfp.fund_approx_irr,
            lfp.fund_tvpi,
            lfp.fund_dpi,
            lfp.num_portfolios,
            lfp.fund_total_invested,
            lfp.fund_total_distributed,
            fs.management_fee_rate,
            fs.carry_rate,
            fs.hurdle_rate,
            fs.vintage_year
           FROM (latest_fund_performance lfp
             JOIN stg_fund_structures fs ON (((lfp.fund_id)::text = fs.fund_id)))
        ), portfolio_details AS (
         SELECT lps.portfolio_id,
            lps.portfolio_name,
            lps.strategy,
            lps.fund_id,
            lps.total_market_value,
            lps.total_unrealized_pnl,
            lps.portfolio_return_pct,
            lps.num_positions,
                CASE
                    WHEN ((EXTRACT(month FROM lps.position_date) >= (1)::numeric) AND (EXTRACT(month FROM lps.position_date) <= (3)::numeric)) THEN 'Q3'::text
                    WHEN ((EXTRACT(month FROM lps.position_date) >= (4)::numeric) AND (EXTRACT(month FROM lps.position_date) <= (6)::numeric)) THEN 'Q4'::text
                    WHEN ((EXTRACT(month FROM lps.position_date) >= (7)::numeric) AND (EXTRACT(month FROM lps.position_date) <= (9)::numeric)) THEN 'Q1'::text
                    WHEN ((EXTRACT(month FROM lps.position_date) >= (10)::numeric) AND (EXTRACT(month FROM lps.position_date) <= (12)::numeric)) THEN 'Q2'::text
                    ELSE NULL::text
                END AS reporting_fiscal_quarter
           FROM latest_portfolio_summary lps
        )
 SELECT fo.fund_id,
    fo.fund_name,
    fo.fund_type,
    fo.vintage_year,
    fo.committed_capital,
    fo.latest_valuation_date,
    fo.fund_nav,
    fo.fund_gav,
    fo.latest_nav_return,
    fo.tvpi_gross,
    fo.fund_approx_irr,
    fo.fund_tvpi,
    fo.fund_dpi,
    fo.num_portfolios,
    fo.fund_total_invested,
    fo.fund_total_distributed,
    fo.management_fee_rate,
    fo.carry_rate,
    fo.hurdle_rate,
    pd.portfolio_id,
    pd.portfolio_name,
    pd.strategy,
    pd.total_market_value AS portfolio_mv,
    pd.total_unrealized_pnl AS portfolio_unrealized_pnl,
    pd.portfolio_return_pct,
    pd.num_positions AS portfolio_num_positions,
    pd.reporting_fiscal_quarter
   FROM (fund_overview fo
     JOIN portfolio_details pd ON (((fo.fund_id)::text = (pd.fund_id)::text)))
  ORDER BY fo.fund_nav DESC, pd.total_market_value DESC;
```

</details>

---

## `public`.`report_lp_quarterly`

**Type:** `VIEW` | **Rows:** 20  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `fund_id` | `TEXT` | YES |  |  |
| 2 | `fund_name` | `TEXT` | YES |  |  |
| 3 | `fund_type` | `TEXT` | YES |  |  |
| 4 | `vintage_year` | `INTEGER` | YES |  |  |
| 5 | `committed_capital` | `NUMERIC(18,2)` | YES |  |  |
| 6 | `management_fee_rate` | `NUMERIC(8,4)` | YES |  |  |
| 7 | `carry_rate` | `NUMERIC(8,4)` | YES |  |  |
| 8 | `hurdle_rate` | `NUMERIC(8,4)` | YES |  |  |
| 9 | `gp_commitment_pct` | `NUMERIC(8,4)` | YES |  |  |
| 10 | `portfolio_id` | `TEXT` | YES |  |  |
| 11 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 12 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 13 | `quarter_start` | `TIMESTAMP WITH TIME ZONE` | YES |  |  |
| 14 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 15 | `cashflow_year` | `NUMERIC` | YES |  |  |
| 16 | `quarter_calls` | `NUMERIC` | YES |  |  |
| 17 | `quarter_distributions` | `NUMERIC` | YES |  |  |
| 18 | `quarter_fees` | `NUMERIC` | YES |  |  |
| 19 | `quarter_net` | `NUMERIC` | YES |  |  |
| 20 | `quarter_end_nav` | `NUMERIC(18,2)` | YES |  |  |
| 21 | `quarter_end_gav` | `NUMERIC(18,2)` | YES |  |  |
| 22 | `unrealized_pnl` | `NUMERIC(18,2)` | YES |  |  |
| 23 | `realized_pnl` | `NUMERIC(18,2)` | YES |  |  |
| 24 | `valuation_method` | `TEXT` | YES |  |  |
| 25 | `approx_irr` | `NUMERIC` | YES |  |  |
| 26 | `tvpi` | `NUMERIC` | YES |  |  |
| 27 | `dpi` | `NUMERIC` | YES |  |  |
| 28 | `rvpi` | `NUMERIC` | YES |  |  |
| 29 | `quarterly_drawdown_rate` | `NUMERIC` | YES |  |  |
| 30 | `nav_to_commitment_ratio` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
WITH quarterly_cashflows AS (
         SELECT cf.fund_id,
            cf.portfolio_id,
            date_trunc('quarter'::text, (cf.cashflow_date)::timestamp with time zone) AS quarter_start,
            cf.cashflow_fiscal_quarter AS fiscal_quarter,
            cf.cashflow_year,
            sum(
                CASE
                    WHEN (cf.cashflow_type = 'capital_call'::text) THEN cf.amount
                    ELSE (0)::numeric
                END) AS quarter_calls,
            sum(
                CASE
                    WHEN (cf.cashflow_type = 'distribution'::text) THEN cf.amount
                    ELSE (0)::numeric
                END) AS quarter_distributions,
            sum(
                CASE
                    WHEN (cf.cashflow_type = 'management_fee'::text) THEN cf.amount
                    ELSE (0)::numeric
                END) AS quarter_fees,
            sum(cf.amount) AS quarter_net
           FROM stg_cashflows cf
          GROUP BY cf.fund_id, cf.portfolio_id, (date_trunc('quarter'::text, (cf.cashflow_date)::timestamp with time zone)), cf.cashflow_fiscal_quarter, cf.cashflow_year
        ), quarterly_valuations AS (
         SELECT sub.portfolio_id,
            sub.valuation_date,
            sub.net_asset_value,
            sub.gross_asset_value,
            sub.unrealized_pnl,
            sub.realized_pnl,
            sub.valuation_method,
            sub.rn
           FROM ( SELECT v.portfolio_id,
                    v.valuation_date,
                    v.net_asset_value,
                    v.gross_asset_value,
                    v.unrealized_pnl,
                    v.realized_pnl,
                    v.valuation_method,
                    row_number() OVER (PARTITION BY v.portfolio_id, (date_trunc('quarter'::text, (v.valuation_date)::timestamp with time zone)) ORDER BY v.valuation_date DESC) AS rn
                   FROM stg_valuations v) sub
          WHERE (sub.rn = 1)
        ), fund_details AS (
         SELECT fs.fund_id,
            fs.fund_name,
            fs.fund_type,
            fs.committed_capital,
            fs.management_fee_rate,
            fs.carry_rate,
            fs.hurdle_rate,
            fs.vintage_year,
            fs.gp_commitment_pct
           FROM stg_fund_structures fs
        ), lp_report AS (
         SELECT fd.fund_id,
            fd.fund_name,
            fd.fund_type,
            fd.vintage_year,
            fd.committed_capital,
            fd.management_fee_rate,
            fd.carry_rate,
            fd.hurdle_rate,
            fd.gp_commitment_pct,
            qc.portfolio_id,
            p.portfolio_name,
            p.strategy,
            qc.quarter_start,
            qc.fiscal_quarter,
            qc.cashflow_year,
            qc.quarter_calls,
            qc.quarter_distributions,
            qc.quarter_fees,
            qc.quarter_net,
            qv.net_asset_value AS quarter_end_nav,
            qv.gross_asset_value AS quarter_end_gav,
            qv.unrealized_pnl,
            qv.realized_pnl,
            qv.valuation_method,
            irr.approx_irr,
            irr.tvpi,
            irr.dpi,
            irr.rvpi
           FROM ((((quarterly_cashflows qc
             JOIN fund_details fd ON ((qc.fund_id = fd.fund_id)))
             JOIN stg_portfolios p ON ((qc.portfolio_id = (p.portfolio_id)::text)))
             LEFT JOIN quarterly_valuations qv ON (((qc.portfolio_id = qv.portfolio_id) AND (date_trunc('quarter'::text, (qv.valuation_date)::timestamp with time zone) = qc.quarter_start))))
             LEFT JOIN int_irr_calculations irr ON (((qc.fund_id = irr.fund_id) AND (qc.portfolio_id = irr.portfolio_id))))
        )
 SELECT fund_id,
    fund_name,
    fund_type,
    vintage_year,
    committed_capital,
    management_fee_rate,
    carry_rate,
    hurdle_rate,
    gp_commitment_pct,
    portfolio_id,
    portfolio_name,
    strategy,
    quarter_start,
    fiscal_quarter,
    cashflow_year,
    quarter_calls,
    quarter_distributions,
    quarter_fees,
    quarter_net,
    quarter_end_nav,
    quarter_end_gav,
    unrealized_pnl,
    realized_pnl,
    valuation_method,
    approx_irr,
    tvpi,
    dpi,
    rvpi,
        CASE
            WHEN (committed_capital > (0)::numeric) THEN (quarter_calls / committed_capital)
            ELSE (0)::numeric
        END AS quarterly_drawdown_rate,
        CASE
            WHEN ((quarter_end_nav IS NOT NULL) AND (committed_capital > (0)::numeric)) THEN (quarter_end_nav / committed_capital)
            ELSE NULL::numeric
        END AS nav_to_commitment_ratio
   FROM lp_report
  ORDER BY fund_id, portfolio_id, quarter_start;
```

</details>

---

## `public`.`report_portfolio_overview`

**Type:** `VIEW` | **Rows:** 5  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `portfolio_id` | `VARCHAR(10)` | YES |  |  |
| 2 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 3 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 4 | `manager_name` | `VARCHAR(100)` | YES |  |  |
| 5 | `reporting_date` | `DATE` | YES |  |  |
| 6 | `num_positions` | `BIGINT` | YES |  |  |
| 7 | `total_market_value` | `NUMERIC` | YES |  |  |
| 8 | `total_cost_basis` | `NUMERIC` | YES |  |  |
| 9 | `total_unrealized_pnl` | `NUMERIC` | YES |  |  |
| 10 | `portfolio_return_pct` | `NUMERIC` | YES |  |  |
| 11 | `largest_position_mv` | `NUMERIC` | YES |  |  |
| 12 | `smallest_position_mv` | `NUMERIC` | YES |  |  |
| 13 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 14 | `fiscal_year` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT portfolio_id,
    portfolio_name,
    strategy,
    manager_name,
    position_date AS reporting_date,
    num_positions,
    total_market_value,
    total_cost_basis,
    total_unrealized_pnl,
    portfolio_return_pct,
    largest_position_mv,
    smallest_position_mv,
        CASE
            WHEN ((EXTRACT(month FROM position_date) >= (1)::numeric) AND (EXTRACT(month FROM position_date) <= (3)::numeric)) THEN 'Q3'::text
            WHEN ((EXTRACT(month FROM position_date) >= (4)::numeric) AND (EXTRACT(month FROM position_date) <= (6)::numeric)) THEN 'Q4'::text
            WHEN ((EXTRACT(month FROM position_date) >= (7)::numeric) AND (EXTRACT(month FROM position_date) <= (9)::numeric)) THEN 'Q1'::text
            WHEN ((EXTRACT(month FROM position_date) >= (10)::numeric) AND (EXTRACT(month FROM position_date) <= (12)::numeric)) THEN 'Q2'::text
            ELSE NULL::text
        END AS fiscal_quarter,
        CASE
            WHEN (EXTRACT(month FROM position_date) >= (7)::numeric) THEN (EXTRACT(year FROM position_date) + (1)::numeric)
            ELSE EXTRACT(year FROM position_date)
        END AS fiscal_year
   FROM fact_portfolio_summary fps
  WHERE (position_date = ( SELECT max(fps2.position_date) AS max
           FROM fact_portfolio_summary fps2
          WHERE ((fps2.portfolio_id)::text = (fps.portfolio_id)::text)))
  ORDER BY total_market_value DESC;
```

</details>

---

## `public`.`stg_benchmarks`

**Type:** `VIEW` | **Rows:** 35  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `benchmark_id` | `TEXT` | YES |  |  |
| 2 | `benchmark_name` | `TEXT` | YES |  |  |
| 3 | `benchmark_date` | `DATE` | YES |  |  |
| 4 | `return_daily` | `NUMERIC(18,8)` | YES |  |  |
| 5 | `return_mtd` | `NUMERIC(18,8)` | YES |  |  |
| 6 | `return_ytd` | `NUMERIC(18,8)` | YES |  |  |
| 7 | `benchmark_level` | `NUMERIC(18,4)` | YES |  |  |
| 8 | `benchmark_fiscal_quarter` | `TEXT` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT benchmark_id,
    benchmark_name,
    benchmark_date,
    (return_daily)::numeric(18,8) AS return_daily,
    (return_mtd)::numeric(18,8) AS return_mtd,
    (return_ytd)::numeric(18,8) AS return_ytd,
    (benchmark_level)::numeric(18,4) AS benchmark_level,
        CASE
            WHEN ((EXTRACT(month FROM benchmark_date) >= (1)::numeric) AND (EXTRACT(month FROM benchmark_date) <= (3)::numeric)) THEN 'Q3'::text
            WHEN ((EXTRACT(month FROM benchmark_date) >= (4)::numeric) AND (EXTRACT(month FROM benchmark_date) <= (6)::numeric)) THEN 'Q4'::text
            WHEN ((EXTRACT(month FROM benchmark_date) >= (7)::numeric) AND (EXTRACT(month FROM benchmark_date) <= (9)::numeric)) THEN 'Q1'::text
            WHEN ((EXTRACT(month FROM benchmark_date) >= (10)::numeric) AND (EXTRACT(month FROM benchmark_date) <= (12)::numeric)) THEN 'Q2'::text
            ELSE NULL::text
        END AS benchmark_fiscal_quarter
   FROM public_raw.raw_benchmarks;
```

</details>

---

## `public`.`stg_cashflows`

**Type:** `VIEW` | **Rows:** 25  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `cashflow_id` | `TEXT` | YES |  |  |
| 2 | `fund_id` | `TEXT` | YES |  |  |
| 3 | `portfolio_id` | `TEXT` | YES |  |  |
| 4 | `cashflow_date` | `DATE` | YES |  |  |
| 5 | `cashflow_type` | `TEXT` | YES |  |  |
| 6 | `amount` | `NUMERIC(18,2)` | YES |  |  |
| 7 | `currency` | `TEXT` | YES |  |  |
| 8 | `description` | `TEXT` | YES |  |  |
| 9 | `investor_id` | `TEXT` | YES |  |  |
| 10 | `cashflow_fiscal_quarter` | `TEXT` | YES |  |  |
| 11 | `cashflow_year` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT cashflow_id,
    fund_id,
    portfolio_id,
    cashflow_date,
    cashflow_type,
    (amount)::numeric(18,2) AS amount,
    currency,
    description,
    investor_id,
        CASE
            WHEN ((EXTRACT(month FROM cashflow_date) >= (1)::numeric) AND (EXTRACT(month FROM cashflow_date) <= (3)::numeric)) THEN 'Q3'::text
            WHEN ((EXTRACT(month FROM cashflow_date) >= (4)::numeric) AND (EXTRACT(month FROM cashflow_date) <= (6)::numeric)) THEN 'Q4'::text
            WHEN ((EXTRACT(month FROM cashflow_date) >= (7)::numeric) AND (EXTRACT(month FROM cashflow_date) <= (9)::numeric)) THEN 'Q1'::text
            WHEN ((EXTRACT(month FROM cashflow_date) >= (10)::numeric) AND (EXTRACT(month FROM cashflow_date) <= (12)::numeric)) THEN 'Q2'::text
            ELSE NULL::text
        END AS cashflow_fiscal_quarter,
    EXTRACT(year FROM cashflow_date) AS cashflow_year
   FROM public_raw.raw_cashflows;
```

</details>

---

## `public`.`stg_counterparties`

**Type:** `VIEW` | **Rows:** 10  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `counterparty_id` | `TEXT` | YES |  |  |
| 2 | `counterparty_name` | `TEXT` | YES |  |  |
| 3 | `counterparty_type` | `TEXT` | YES |  |  |
| 4 | `country` | `TEXT` | YES |  |  |
| 5 | `credit_rating` | `TEXT` | YES |  |  |
| 6 | `is_active` | `BOOLEAN` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT counterparty_id,
    counterparty_name,
    counterparty_type,
    country,
    credit_rating,
    is_active
   FROM public_raw.raw_counterparties
  WHERE (is_active = true);
```

</details>

---

## `public`.`stg_dates`

**Type:** `VIEW` | **Rows:** 15  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `date_day` | `DATE` | YES |  |  |
| 2 | `is_business_day` | `BOOLEAN` | YES |  |  |
| 3 | `is_month_end` | `BOOLEAN` | YES |  |  |
| 4 | `is_quarter_end` | `BOOLEAN` | YES |  |  |
| 5 | `is_year_end` | `BOOLEAN` | YES |  |  |
| 6 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 7 | `fiscal_year` | `TEXT` | YES |  |  |
| 8 | `month_start` | `TIMESTAMP WITH TIME ZONE` | YES |  |  |
| 9 | `quarter_start` | `TIMESTAMP WITH TIME ZONE` | YES |  |  |
| 10 | `calendar_month` | `NUMERIC` | YES |  |  |
| 11 | `calendar_year` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT date_day,
    is_business_day,
    is_month_end,
    is_quarter_end,
    is_year_end,
    fiscal_quarter,
    fiscal_year,
    date_trunc('month'::text, (date_day)::timestamp with time zone) AS month_start,
    date_trunc('quarter'::text, (date_day)::timestamp with time zone) AS quarter_start,
    EXTRACT(month FROM date_day) AS calendar_month,
    EXTRACT(year FROM date_day) AS calendar_year
   FROM public_raw.raw_dates;
```

</details>

---

## `public`.`stg_fund_structures`

**Type:** `VIEW` | **Rows:** 3  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `fund_id` | `TEXT` | YES |  |  |
| 2 | `fund_name` | `TEXT` | YES |  |  |
| 3 | `fund_type` | `TEXT` | YES |  |  |
| 4 | `vintage_year` | `INTEGER` | YES |  |  |
| 5 | `committed_capital` | `NUMERIC(18,2)` | YES |  |  |
| 6 | `management_fee_rate` | `NUMERIC(8,4)` | YES |  |  |
| 7 | `carry_rate` | `NUMERIC(8,4)` | YES |  |  |
| 8 | `hurdle_rate` | `NUMERIC(8,4)` | YES |  |  |
| 9 | `gp_commitment_pct` | `NUMERIC(8,4)` | YES |  |  |
| 10 | `fund_currency` | `TEXT` | YES |  |  |
| 11 | `fund_status` | `TEXT` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT fund_id,
    fund_name,
    fund_type,
    vintage_year,
    (committed_capital)::numeric(18,2) AS committed_capital,
    (management_fee_rate)::numeric(8,4) AS management_fee_rate,
    (carry_rate)::numeric(8,4) AS carry_rate,
    (hurdle_rate)::numeric(8,4) AS hurdle_rate,
    (gp_commitment_pct)::numeric(8,4) AS gp_commitment_pct,
    fund_currency,
    fund_status
   FROM public_raw.raw_fund_structures;
```

</details>

---

## `public`.`stg_instruments`

**Type:** `VIEW` | **Rows:** 20  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `instrument_id` | `TEXT` | YES |  |  |
| 2 | `instrument_name` | `TEXT` | YES |  |  |
| 3 | `instrument_type` | `TEXT` | YES |  |  |
| 4 | `sector` | `TEXT` | YES |  |  |
| 5 | `currency` | `TEXT` | YES |  |  |
| 6 | `exchange` | `TEXT` | YES |  |  |
| 7 | `issuer` | `TEXT` | YES |  |  |
| 8 | `maturity_date` | `DATE` | YES |  |  |
| 9 | `days_to_maturity` | `INTEGER` | YES |  |  |
| 10 | `liquidity_class` | `TEXT` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT instrument_id,
    instrument_name,
    instrument_type,
    sector,
    currency,
    exchange,
    issuer,
    maturity_date,
        CASE
            WHEN (maturity_date IS NOT NULL) THEN (maturity_date - CURRENT_DATE)
            ELSE NULL::integer
        END AS days_to_maturity,
        CASE
            WHEN (instrument_type = ANY (ARRAY['equity'::text, 'futures'::text])) THEN 'liquid'::text
            WHEN (instrument_type = ANY (ARRAY['corporate_bond'::text, 'government_bond'::text, 'sovereign_bond'::text, 'leveraged_loan'::text])) THEN 'semi_liquid'::text
            WHEN (instrument_type = ANY (ARRAY['private_equity'::text, 'venture_capital'::text, 'real_estate'::text, 'real_estate_debt'::text, 'infrastructure'::text])) THEN 'illiquid'::text
            ELSE 'other'::text
        END AS liquidity_class
   FROM public_raw.raw_instruments;
```

</details>

---

## `public`.`stg_portfolios`

**Type:** `VIEW` | **Rows:** 5  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `portfolio_id` | `VARCHAR(10)` | YES |  |  |
| 2 | `portfolio_name` | `VARCHAR(100)` | YES |  |  |
| 3 | `strategy` | `VARCHAR(50)` | YES |  |  |
| 4 | `inception_date` | `DATE` | YES |  |  |
| 5 | `fund_id` | `VARCHAR(10)` | YES |  |  |
| 6 | `manager_name` | `VARCHAR(100)` | YES |  |  |
| 7 | `is_active` | `BOOLEAN` | YES |  |  |
| 8 | `inception_fiscal_quarter` | `TEXT` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT (portfolio_id)::character varying(10) AS portfolio_id,
    (portfolio_name)::character varying(100) AS portfolio_name,
    (strategy)::character varying(50) AS strategy,
    inception_date,
    (fund_id)::character varying(10) AS fund_id,
    (manager_name)::character varying(100) AS manager_name,
    is_active,
        CASE
            WHEN ((EXTRACT(month FROM inception_date) >= (1)::numeric) AND (EXTRACT(month FROM inception_date) <= (3)::numeric)) THEN 'Q3'::text
            WHEN ((EXTRACT(month FROM inception_date) >= (4)::numeric) AND (EXTRACT(month FROM inception_date) <= (6)::numeric)) THEN 'Q4'::text
            WHEN ((EXTRACT(month FROM inception_date) >= (7)::numeric) AND (EXTRACT(month FROM inception_date) <= (9)::numeric)) THEN 'Q1'::text
            WHEN ((EXTRACT(month FROM inception_date) >= (10)::numeric) AND (EXTRACT(month FROM inception_date) <= (12)::numeric)) THEN 'Q2'::text
            ELSE NULL::text
        END AS inception_fiscal_quarter
   FROM public_raw.raw_portfolios
  WHERE (is_active = true);
```

</details>

---

## `public`.`stg_positions`

**Type:** `VIEW` | **Rows:** 30  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `position_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `instrument_id` | `TEXT` | YES |  |  |
| 4 | `position_date` | `DATE` | YES |  |  |
| 5 | `quantity` | `NUMERIC(18,4)` | YES |  |  |
| 6 | `market_value` | `NUMERIC(18,2)` | YES |  |  |
| 7 | `cost_basis` | `NUMERIC(18,2)` | YES |  |  |
| 8 | `currency` | `TEXT` | YES |  |  |
| 9 | `unrealized_gain_loss` | `NUMERIC` | YES |  |  |
| 10 | `unrealized_return_pct` | `NUMERIC` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT position_id,
    portfolio_id,
    instrument_id,
    position_date,
    (quantity)::numeric(18,4) AS quantity,
    (market_value)::numeric(18,2) AS market_value,
    (cost_basis)::numeric(18,2) AS cost_basis,
    currency,
    ((market_value)::numeric(18,2) - (cost_basis)::numeric(18,2)) AS unrealized_gain_loss,
        CASE
            WHEN ((cost_basis)::numeric(18,2) <> (0)::numeric) THEN (((market_value)::numeric(18,2) - (cost_basis)::numeric(18,2)) / abs((cost_basis)::numeric(18,2)))
            ELSE (0)::numeric
        END AS unrealized_return_pct
   FROM public_raw.raw_positions;
```

</details>

---

## `public`.`stg_trades`

**Type:** `VIEW` | **Rows:** 40  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `trade_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `instrument_id` | `TEXT` | YES |  |  |
| 4 | `counterparty_id` | `TEXT` | YES |  |  |
| 5 | `trade_date` | `DATE` | YES |  |  |
| 6 | `settlement_date` | `DATE` | YES |  |  |
| 7 | `trade_type` | `TEXT` | YES |  |  |
| 8 | `quantity` | `NUMERIC(18,4)` | YES |  |  |
| 9 | `price` | `NUMERIC(18,6)` | YES |  |  |
| 10 | `notional_amount` | `NUMERIC(18,2)` | YES |  |  |
| 11 | `commission` | `NUMERIC(18,2)` | YES |  |  |
| 12 | `currency` | `TEXT` | YES |  |  |
| 13 | `status` | `TEXT` | YES |  |  |
| 14 | `trade_fiscal_quarter` | `TEXT` | YES |  |  |
| 15 | `settlement_days` | `INTEGER` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT trade_id,
    portfolio_id,
    instrument_id,
    counterparty_id,
    trade_date,
    settlement_date,
    trade_type,
    (quantity)::numeric(18,4) AS quantity,
    (price)::numeric(18,6) AS price,
    (notional_amount)::numeric(18,2) AS notional_amount,
    (commission)::numeric(18,2) AS commission,
    currency,
    status,
        CASE
            WHEN ((EXTRACT(month FROM trade_date) >= (1)::numeric) AND (EXTRACT(month FROM trade_date) <= (3)::numeric)) THEN 'Q3'::text
            WHEN ((EXTRACT(month FROM trade_date) >= (4)::numeric) AND (EXTRACT(month FROM trade_date) <= (6)::numeric)) THEN 'Q4'::text
            WHEN ((EXTRACT(month FROM trade_date) >= (7)::numeric) AND (EXTRACT(month FROM trade_date) <= (9)::numeric)) THEN 'Q1'::text
            WHEN ((EXTRACT(month FROM trade_date) >= (10)::numeric) AND (EXTRACT(month FROM trade_date) <= (12)::numeric)) THEN 'Q2'::text
            ELSE NULL::text
        END AS trade_fiscal_quarter,
    (settlement_date - trade_date) AS settlement_days
   FROM public_raw.raw_trades
  WHERE (status = 'settled'::text);
```

</details>

---

## `public`.`stg_valuations`

**Type:** `VIEW` | **Rows:** 21  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `valuation_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `valuation_date` | `DATE` | YES |  |  |
| 4 | `gross_asset_value` | `NUMERIC(18,2)` | YES |  |  |
| 5 | `net_asset_value` | `NUMERIC(18,2)` | YES |  |  |
| 6 | `total_liabilities` | `NUMERIC(18,2)` | YES |  |  |
| 7 | `unrealized_pnl` | `NUMERIC(18,2)` | YES |  |  |
| 8 | `realized_pnl` | `NUMERIC(18,2)` | YES |  |  |
| 9 | `valuation_method` | `TEXT` | YES |  |  |
| 10 | `valuation_fiscal_quarter` | `TEXT` | YES |  |  |

<details>
<summary>View Definition (click to expand)</summary>

```sql
SELECT DISTINCT valuation_id,
    portfolio_id,
    valuation_date,
    (gross_asset_value)::numeric(18,2) AS gross_asset_value,
    (net_asset_value)::numeric(18,2) AS net_asset_value,
    (total_liabilities)::numeric(18,2) AS total_liabilities,
    (unrealized_pnl)::numeric(18,2) AS unrealized_pnl,
    (realized_pnl)::numeric(18,2) AS realized_pnl,
    valuation_method,
        CASE
            WHEN ((EXTRACT(month FROM valuation_date) >= (1)::numeric) AND (EXTRACT(month FROM valuation_date) <= (3)::numeric)) THEN 'Q3'::text
            WHEN ((EXTRACT(month FROM valuation_date) >= (4)::numeric) AND (EXTRACT(month FROM valuation_date) <= (6)::numeric)) THEN 'Q4'::text
            WHEN ((EXTRACT(month FROM valuation_date) >= (7)::numeric) AND (EXTRACT(month FROM valuation_date) <= (9)::numeric)) THEN 'Q1'::text
            WHEN ((EXTRACT(month FROM valuation_date) >= (10)::numeric) AND (EXTRACT(month FROM valuation_date) <= (12)::numeric)) THEN 'Q2'::text
            ELSE NULL::text
        END AS valuation_fiscal_quarter
   FROM public_raw.raw_valuations;
```

</details>

---

## `public_raw`.`raw_benchmarks`

**Type:** `TABLE` | **Rows:** 35  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `benchmark_id` | `TEXT` | YES |  |  |
| 2 | `benchmark_name` | `TEXT` | YES |  |  |
| 3 | `benchmark_date` | `DATE` | YES |  |  |
| 4 | `return_daily` | `DOUBLE PRECISION` | YES |  |  |
| 5 | `return_mtd` | `DOUBLE PRECISION` | YES |  |  |
| 6 | `return_ytd` | `DOUBLE PRECISION` | YES |  |  |
| 7 | `benchmark_level` | `DOUBLE PRECISION` | YES |  |  |

---

## `public_raw`.`raw_cashflows`

**Type:** `TABLE` | **Rows:** 25  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `cashflow_id` | `TEXT` | YES |  |  |
| 2 | `fund_id` | `TEXT` | YES |  |  |
| 3 | `portfolio_id` | `TEXT` | YES |  |  |
| 4 | `cashflow_date` | `DATE` | YES |  |  |
| 5 | `cashflow_type` | `TEXT` | YES |  |  |
| 6 | `amount` | `INTEGER` | YES |  |  |
| 7 | `currency` | `TEXT` | YES |  |  |
| 8 | `description` | `TEXT` | YES |  |  |
| 9 | `investor_id` | `TEXT` | YES |  |  |

---

## `public_raw`.`raw_counterparties`

**Type:** `TABLE` | **Rows:** 10  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `counterparty_id` | `TEXT` | YES |  |  |
| 2 | `counterparty_name` | `TEXT` | YES |  |  |
| 3 | `counterparty_type` | `TEXT` | YES |  |  |
| 4 | `country` | `TEXT` | YES |  |  |
| 5 | `credit_rating` | `TEXT` | YES |  |  |
| 6 | `is_active` | `BOOLEAN` | YES |  |  |

---

## `public_raw`.`raw_dates`

**Type:** `TABLE` | **Rows:** 15  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `date_day` | `DATE` | YES |  |  |
| 2 | `is_business_day` | `BOOLEAN` | YES |  |  |
| 3 | `is_month_end` | `BOOLEAN` | YES |  |  |
| 4 | `is_quarter_end` | `BOOLEAN` | YES |  |  |
| 5 | `is_year_end` | `BOOLEAN` | YES |  |  |
| 6 | `fiscal_quarter` | `TEXT` | YES |  |  |
| 7 | `fiscal_year` | `TEXT` | YES |  |  |

---

## `public_raw`.`raw_fund_structures`

**Type:** `TABLE` | **Rows:** 3  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `fund_id` | `TEXT` | YES |  |  |
| 2 | `fund_name` | `TEXT` | YES |  |  |
| 3 | `fund_type` | `TEXT` | YES |  |  |
| 4 | `vintage_year` | `INTEGER` | YES |  |  |
| 5 | `committed_capital` | `INTEGER` | YES |  |  |
| 6 | `management_fee_rate` | `DOUBLE PRECISION` | YES |  |  |
| 7 | `carry_rate` | `DOUBLE PRECISION` | YES |  |  |
| 8 | `hurdle_rate` | `DOUBLE PRECISION` | YES |  |  |
| 9 | `gp_commitment_pct` | `DOUBLE PRECISION` | YES |  |  |
| 10 | `fund_currency` | `TEXT` | YES |  |  |
| 11 | `fund_status` | `TEXT` | YES |  |  |

---

## `public_raw`.`raw_instruments`

**Type:** `TABLE` | **Rows:** 20  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `instrument_id` | `TEXT` | YES |  |  |
| 2 | `instrument_name` | `TEXT` | YES |  |  |
| 3 | `instrument_type` | `TEXT` | YES |  |  |
| 4 | `sector` | `TEXT` | YES |  |  |
| 5 | `currency` | `TEXT` | YES |  |  |
| 6 | `exchange` | `TEXT` | YES |  |  |
| 7 | `issuer` | `TEXT` | YES |  |  |
| 8 | `maturity_date` | `DATE` | YES |  |  |

---

## `public_raw`.`raw_portfolios`

**Type:** `TABLE` | **Rows:** 5  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `portfolio_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_name` | `TEXT` | YES |  |  |
| 3 | `strategy` | `TEXT` | YES |  |  |
| 4 | `inception_date` | `DATE` | YES |  |  |
| 5 | `fund_id` | `TEXT` | YES |  |  |
| 6 | `manager_name` | `TEXT` | YES |  |  |
| 7 | `is_active` | `BOOLEAN` | YES |  |  |

---

## `public_raw`.`raw_positions`

**Type:** `TABLE` | **Rows:** 30  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `position_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `instrument_id` | `TEXT` | YES |  |  |
| 4 | `position_date` | `DATE` | YES |  |  |
| 5 | `quantity` | `INTEGER` | YES |  |  |
| 6 | `market_value` | `INTEGER` | YES |  |  |
| 7 | `cost_basis` | `INTEGER` | YES |  |  |
| 8 | `currency` | `TEXT` | YES |  |  |

---

## `public_raw`.`raw_trades`

**Type:** `TABLE` | **Rows:** 40  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `trade_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `instrument_id` | `TEXT` | YES |  |  |
| 4 | `counterparty_id` | `TEXT` | YES |  |  |
| 5 | `trade_date` | `DATE` | YES |  |  |
| 6 | `settlement_date` | `DATE` | YES |  |  |
| 7 | `trade_type` | `TEXT` | YES |  |  |
| 8 | `quantity` | `INTEGER` | YES |  |  |
| 9 | `price` | `DOUBLE PRECISION` | YES |  |  |
| 10 | `notional_amount` | `INTEGER` | YES |  |  |
| 11 | `commission` | `DOUBLE PRECISION` | YES |  |  |
| 12 | `currency` | `TEXT` | YES |  |  |
| 13 | `status` | `TEXT` | YES |  |  |

---

## `public_raw`.`raw_valuations`

**Type:** `TABLE` | **Rows:** 21  

| # | Column | Type | Nullable | Default | PK |
|---|--------|------|----------|---------|-----|
| 1 | `valuation_id` | `TEXT` | YES |  |  |
| 2 | `portfolio_id` | `TEXT` | YES |  |  |
| 3 | `valuation_date` | `DATE` | YES |  |  |
| 4 | `gross_asset_value` | `INTEGER` | YES |  |  |
| 5 | `net_asset_value` | `INTEGER` | YES |  |  |
| 6 | `total_liabilities` | `INTEGER` | YES |  |  |
| 7 | `unrealized_pnl` | `INTEGER` | YES |  |  |
| 8 | `realized_pnl` | `INTEGER` | YES |  |  |
| 9 | `valuation_method` | `TEXT` | YES |  |  |

---
