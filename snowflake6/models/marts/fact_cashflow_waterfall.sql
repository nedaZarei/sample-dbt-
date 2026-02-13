|-- Pipeline C: FACT table - Cashflow waterfall for PE/fund reporting
|-- Anti-pattern: deep CTEs, repeated joins, late aggregation
|-- Anti-pattern: PostgreSQL-specific date functions
|
|with enriched_cashflows as (
|    select
|        ce.cashflow_id,
|        ce.fund_id,
|        ce.fund_name,
|        ce.fund_type,
|        ce.portfolio_id,
|        ce.portfolio_name,
|        ce.cashflow_date,
|        ce.cashflow_type,
|        ce.amount,
|        ce.investor_id,
|        ce.committed_capital,
|        ce.management_fee_rate,
|        ce.carry_rate,
|        ce.hurdle_rate,
|        ce.cumulative_called,
|        ce.cumulative_distributed,
|        ce.cumulative_net_cashflow,
|        ce.pct_called,
|        -- Anti-pattern: duplicated fiscal quarter logic
|        case
|            when extract(month from ce.cashflow_date) between 1 and 3 then 'Q3'
|            when extract(month from ce.cashflow_date) between 4 and 6 then 'Q4'
|            when extract(month from ce.cashflow_date) between 7 and 9 then 'Q1'
|            when extract(month from ce.cashflow_date) between 10 and 12 then 'Q2'
|        end as fiscal_quarter,
|        -- Anti-pattern: PostgreSQL extract
|        extract(year from ce.cashflow_date) as calendar_year
|    from {{ ref('int_cashflow_enriched') }} ce
|),
|
|-- Anti-pattern: re-joining valuations when int_valuation_enriched already has this
|waterfall_with_nav as (
|    select
|        ecf.*,
|        v.net_asset_value,
|        v.gross_asset_value,
|        v.unrealized_pnl as nav_unrealized_pnl,
|        v.realized_pnl as nav_realized_pnl
|    from enriched_cashflows ecf
|    left join {{ ref('stg_valuations') }} v
|        on ecf.portfolio_id = v.portfolio_id
|        and ecf.cashflow_date = v.valuation_date
|),
|
|-- Anti-pattern: computing carry/waterfall in a CTE chain instead of a macro
|waterfall_calculations as (
|    select
|        wn.*,
|        -- Simplified carry calculation
|        case
|            when wn.cumulative_distributed > wn.cumulative_called * (1 + wn.hurdle_rate)
|            then (wn.cumulative_distributed - wn.cumulative_called * (1 + wn.hurdle_rate)) * wn.carry_rate
|            else 0
|        end as estimated_carry,
|        wn.cumulative_called * wn.management_fee_rate as annual_mgmt_fee,
|        case
|            when wn.cumulative_called > 0
|            then wn.cumulative_distributed / wn.cumulative_called
|            else 0
|        end as dpi,
|        case
|            when wn.cumulative_called > 0 and wn.net_asset_value is not null
|            then (wn.cumulative_distributed + wn.net_asset_value) / wn.cumulative_called
|            else null
|        end as tvpi
|    from waterfall_with_nav wn
|)
|
|select * from waterfall_calculations
