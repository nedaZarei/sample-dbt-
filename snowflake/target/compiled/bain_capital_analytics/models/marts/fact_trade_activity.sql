-- Pipeline B: FACT table - Trade activity detail

select
    t.trade_id,
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
    case
        when extract(month from t.trade_date) between 1 and 3 then 'Q3'
        when extract(month from t.trade_date) between 4 and 6 then 'Q4'
        when extract(month from t.trade_date) between 7 and 9 then 'Q1'
        when extract(month from t.trade_date) between 10 and 12 then 'Q2'
    end as computed_fiscal_quarter,
    sum(t.notional_amount) over (
        partition by t.portfolio_id
        order by t.trade_date
        rows between unbounded preceding and current row
    ) as running_notional,
    count(*) over (
        partition by t.portfolio_id, trunc(t.trade_date, 'month')
    ) as monthly_trade_count,
    sum(t.commission) over (
        partition by t.portfolio_id, trunc(t.trade_date, 'month')
    ) as monthly_commissions
from DBT_DEMO.DEV.stg_trades t
inner join DBT_DEMO.DEV.stg_instruments i
    on t.instrument_id = i.instrument_id
inner join DBT_DEMO.DEV.stg_counterparties c
    on t.counterparty_id = c.counterparty_id
inner join DBT_DEMO.DEV.stg_portfolios p
    on t.portfolio_id = p.portfolio_id
inner join DBT_DEMO.DEV.stg_dates d
    on t.trade_date = d.date_day