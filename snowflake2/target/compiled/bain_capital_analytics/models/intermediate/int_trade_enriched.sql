-- Pipeline B: Enriched trade data with instrument and counterparty details
-- Anti-pattern: subquery for dedup instead of QUALIFY (Snowflake)
-- Anti-pattern: heavy join that will be repeated in multiple downstream models

with latest_trades as (
    -- Anti-pattern: subquery to filter row_number instead of QUALIFY
    select *
    from (
        select
            t.*,
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
            -- Anti-pattern: duplicated fiscal quarter logic AGAIN
            case
                when extract(month from t.trade_date) between 1 and 3 then 'Q3'
                when extract(month from t.trade_date) between 4 and 6 then 'Q4'
                when extract(month from t.trade_date) between 7 and 9 then 'Q1'
                when extract(month from t.trade_date) between 10 and 12 then 'Q2'
            end as fiscal_quarter,
            row_number() over (
                partition by t.portfolio_id, t.instrument_id, t.trade_date
                order by t.trade_id desc
            ) as rn
        from DBT_DEMO.DEV.stg_trades t
        -- Anti-pattern: these 3 joins are repeated in fact_portfolio_pnl and fact_trade_activity
        inner join DBT_DEMO.DEV.stg_instruments i
            on t.instrument_id = i.instrument_id
        inner join DBT_DEMO.DEV.stg_counterparties c
            on t.counterparty_id = c.counterparty_id
        inner join DBT_DEMO.DEV.stg_portfolios p
            on t.portfolio_id = p.portfolio_id
    ) sub
    where rn = 1
)

select
    trade_id,
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
    -- Anti-pattern: running total computed here AND again in downstream models
    sum(notional_amount) over (
        partition by portfolio_id
        order by trade_date
        rows between unbounded preceding and current row
    ) as cumulative_notional
from latest_trades