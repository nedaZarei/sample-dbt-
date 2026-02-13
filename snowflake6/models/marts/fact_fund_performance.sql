|-- Pipeline C: FACT table - Fund-level performance metrics
|-- Anti-pattern: late aggregation - joins everything then aggregates
|-- Anti-pattern: re-joins fund_structures (already in int_fund_nav, int_cashflow_enriched)
|-- Anti-pattern: materialized as view (should be table)
|
|with fund_nav_data as (
|    select
|        fn.fund_id,
|        fn.fund_name,
|        fn.fund_type,
|        fn.committed_capital,
|        fn.valuation_date,
|        fn.fund_nav,
|        fn.fund_gav,
|        fn.fund_total_liabilities,
|        fn.fund_unrealized_pnl,
|        fn.fund_realized_pnl,
|        fn.fund_nav_return,
|        fn.tvpi_gross,
|        fn.num_portfolios
|    from {{ ref('int_fund_nav') }} fn
|),
|
|fund_cashflows as (
|    -- Anti-pattern: re-aggregating cashflows that are already in int_cashflow_enriched
|    select
|        cf.fund_id,
|        cf.cashflow_date,
|        sum(case when cf.cashflow_type = 'capital_call' then cf.amount else 0 end) as period_calls,
|        sum(case when cf.cashflow_type = 'distribution' then cf.amount else 0 end) as period_distributions,
|        sum(case when cf.cashflow_type = 'management_fee' then cf.amount else 0 end) as period_fees,
|        sum(cf.amount) as period_net_cashflow
|    from {{ ref('stg_cashflows') }} cf
|    group by cf.fund_id, cf.cashflow_date
|),
|
|fund_irr as (
|    select
|        irr.fund_id,
|        sum(irr.total_invested) as fund_total_invested,
|        sum(irr.total_distributed) as fund_total_distributed,
|        sum(irr.terminal_value) as fund_terminal_value,
|        -- Anti-pattern: averaging portfolio IRRs as proxy for fund IRR (incorrect but intentional)
|        avg(irr.approx_irr) as fund_approx_irr,
|        avg(irr.tvpi) as fund_tvpi,
|        avg(irr.dpi) as fund_dpi
|    from {{ ref('int_irr_calculations') }} irr
|    group by irr.fund_id
|),
|
|-- Anti-pattern: joining everything together at the end
|combined as (
|    select
|        fn.fund_id,
|        fn.fund_name,
|        fn.fund_type,
|        fn.committed_capital,
|        fn.valuation_date,
|        fn.fund_nav,
|        fn.fund_gav,
|        fn.fund_total_liabilities,
|        fn.fund_unrealized_pnl,
|        fn.fund_realized_pnl,
|        fn.fund_nav_return,
|        fn.tvpi_gross,
|        fn.num_portfolios,
|        coalesce(fc.period_calls, 0) as period_calls,
|        coalesce(fc.period_distributions, 0) as period_distributions,
|        coalesce(fc.period_fees, 0) as period_fees,
|        coalesce(fc.period_net_cashflow, 0) as period_net_cashflow,
|        fi.fund_total_invested,
|        fi.fund_total_distributed,
|        fi.fund_approx_irr,
|        fi.fund_tvpi,
|        fi.fund_dpi
|    from fund_nav_data fn
|    left join fund_cashflows fc
|        on fn.fund_id = fc.fund_id
|        and fn.valuation_date = fc.cashflow_date
|    left join fund_irr fi
|        on fn.fund_id = fi.fund_id
|)
|
|select * from combined
