-- Pipeline C: Fund-level NAV calculation from portfolio valuations

with portfolio_valuations as (
    select
        v.portfolio_id,
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
    from DBT_DEMO.DEV.stg_valuations v
    inner join DBT_DEMO.DEV.stg_portfolios p
        on v.portfolio_id = p.portfolio_id
    inner join DBT_DEMO.DEV.stg_fund_structures fs
        on p.fund_id = fs.fund_id
),

fund_level_nav as (
    select
        fund_id,
        fund_name,
        fund_type,
        committed_capital,
        management_fee_rate,
        valuation_date,
        sum(net_asset_value) as fund_nav,
        sum(gross_asset_value) as fund_gav,
        sum(total_liabilities) as fund_total_liabilities,
        sum(unrealized_pnl) as fund_unrealized_pnl,
        sum(realized_pnl) as fund_realized_pnl,
        count(distinct portfolio_id) as num_portfolios
    from portfolio_valuations
    group by
        fund_id, fund_name, fund_type, committed_capital,
        management_fee_rate, valuation_date
),

fund_nav_with_changes as (
    select
        fn.*,
        lag(fn.fund_nav) over (
            partition by fn.fund_id order by fn.valuation_date
        ) as prev_fund_nav,
        case
            when lag(fn.fund_nav) over (
                partition by fn.fund_id order by fn.valuation_date
            ) is not null
            and lag(fn.fund_nav) over (
                partition by fn.fund_id order by fn.valuation_date
            ) != 0
            then (fn.fund_nav - lag(fn.fund_nav) over (
                partition by fn.fund_id order by fn.valuation_date
            )) / lag(fn.fund_nav) over (
                partition by fn.fund_id order by fn.valuation_date
            )
            else null
        end as fund_nav_return,
        fn.fund_nav / nullif(fn.committed_capital, 0) as tvpi_gross
    from fund_level_nav fn
)

select * from fund_nav_with_changes