-- Pipeline B: FACT table - Portfolio PnL by date
-- Anti-pattern: repeats the same heavy 3-table join as int_trade_enriched
-- Anti-pattern: late aggregation after expensive joins
-- Anti-pattern: materialized as view (should be table/incremental)

with trade_pnl as (
    -- Anti-pattern: re-joining trades to instruments and portfolios
    -- (same join already done in int_trade_enriched)
    select
        t.portfolio_id,
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
    from DBT_DEMO.DEV.stg_trades t
    inner join DBT_DEMO.DEV.stg_instruments i
        on t.instrument_id = i.instrument_id
    inner join DBT_DEMO.DEV.stg_portfolios p
        on t.portfolio_id = p.portfolio_id
    inner join DBT_DEMO.DEV.stg_dates d
        on t.trade_date = d.date_day
),

position_pnl as (
    select
        dp.portfolio_id,
        dp.position_date,
        dp.portfolio_name,
        dp.strategy,
        dp.fund_id,
        dp.fiscal_quarter,
        dp.fiscal_year,
        sum(dp.daily_pnl) as position_daily_pnl,
        sum(dp.market_value) as total_market_value,
        count(distinct dp.instrument_id) as num_positions
    from DBT_DEMO.DEV.int_daily_positions dp
    group by
        dp.portfolio_id, dp.position_date, dp.portfolio_name,
        dp.strategy, dp.fund_id, dp.fiscal_quarter, dp.fiscal_year
),

-- Anti-pattern: aggregating trades separately then joining back
trade_activity as (
    select
        portfolio_id,
        trade_date,
        sum(notional_amount) as total_traded_notional,
        sum(commission) as total_commissions,
        count(*) as num_trades
    from trade_pnl
    group by portfolio_id, trade_date
)

select
    pp.portfolio_id,
    pp.portfolio_name,
    pp.strategy,
    pp.fund_id,
    pp.position_date as pnl_date,
    pp.fiscal_quarter,
    pp.fiscal_year,
    pp.position_daily_pnl,
    pp.total_market_value,
    pp.num_positions,
    coalesce(ta.total_traded_notional, 0) as total_traded_notional,
    coalesce(ta.total_commissions, 0) as total_commissions,
    coalesce(ta.num_trades, 0) as num_trades,
    -- Anti-pattern: running total recomputed here (already in int_trade_enriched)
    sum(pp.position_daily_pnl) over (
        partition by pp.portfolio_id
        order by pp.position_date
        rows between unbounded preceding and current row
    ) as cumulative_pnl
from position_pnl pp
left join trade_activity ta
    on pp.portfolio_id = ta.portfolio_id
    and pp.position_date = ta.trade_date