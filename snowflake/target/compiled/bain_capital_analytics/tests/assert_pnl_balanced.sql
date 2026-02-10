-- Custom test: Ensure total PnL across all portfolios is reasonable
-- (not wildly negative, which would indicate a data or logic error)

select
    pnl_date,
    sum(position_daily_pnl) as total_daily_pnl
from DBT_DEMO.DEV.fact_portfolio_pnl
group by pnl_date
having sum(position_daily_pnl) < -100000000  -- flag if total daily loss exceeds $100M