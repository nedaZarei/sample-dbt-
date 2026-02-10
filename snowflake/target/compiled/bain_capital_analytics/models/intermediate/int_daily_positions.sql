-- Pipeline B: Daily position snapshots with PnL calculations

with position_base as (
    select
        pos.position_id,
        pos.portfolio_id,
        pos.instrument_id,
        pos.position_date,
        pos.quantity,
        pos.market_value,
        pos.cost_basis,
        pos.unrealized_gain_loss,
        pos.unrealized_return_pct,
        pos.currency
    from DBT_DEMO.DEV.stg_positions pos
),

position_with_instruments as (
    select
        pb.*,
        i.instrument_name,
        i.instrument_type,
        i.sector,
        i.liquidity_class
    from position_base pb
    inner join DBT_DEMO.DEV.stg_instruments i
        on pb.instrument_id = i.instrument_id
),

position_with_portfolio as (
    select
        pwi.*,
        p.portfolio_name,
        p.strategy,
        p.fund_id
    from position_with_instruments pwi
    inner join DBT_DEMO.DEV.stg_portfolios p
        on pwi.portfolio_id = p.portfolio_id
),

position_with_dates as (
    select
        pwp.*,
        d.is_business_day,
        d.is_month_end,
        d.is_quarter_end,
        d.fiscal_quarter,
        d.fiscal_year
    from position_with_portfolio pwp
    inner join DBT_DEMO.DEV.stg_dates d
        on pwp.position_date = d.date_day
),

position_with_analytics as (
    select
        pwd.*,
        lag(pwd.market_value) over (
            partition by pwd.portfolio_id, pwd.instrument_id
            order by pwd.position_date
        ) as prev_market_value,
        pwd.market_value - coalesce(
            lag(pwd.market_value) over (
                partition by pwd.portfolio_id, pwd.instrument_id
                order by pwd.position_date
            ), pwd.cost_basis
        ) as daily_pnl,
        sum(pwd.market_value) over (
            partition by pwd.portfolio_id, pwd.position_date
        ) as portfolio_total_mv,
        case
            when sum(pwd.market_value) over (partition by pwd.portfolio_id, pwd.position_date) != 0
            then pwd.market_value / sum(pwd.market_value) over (partition by pwd.portfolio_id, pwd.position_date)
            else 0
        end as weight_in_portfolio
    from position_with_dates pwd
)

select * from position_with_analytics