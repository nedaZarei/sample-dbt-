
  create or replace   view DBT_DEMO.DEV.report_daily_pnl
  
  
  
  
  as (
    -- Pipeline B: REPORT table - Daily PnL report for portfolio managers

with latest_pnl as (
    select
        fpnl.portfolio_id,
        fpnl.portfolio_name,
        fpnl.strategy,
        fpnl.pnl_date,
        fpnl.fiscal_quarter,
        fpnl.position_daily_pnl,
        fpnl.cumulative_pnl,
        fpnl.total_market_value,
        fpnl.num_positions,
        fpnl.total_traded_notional,
        fpnl.total_commissions,
        fpnl.num_trades
    from DBT_DEMO.DEV.fact_portfolio_pnl fpnl
    qualify row_number() over (
        partition by fpnl.portfolio_id
        order by fpnl.pnl_date desc
    ) <= 5
),

pnl_with_benchmark as (
    select
        lp.*,
        bm.return_mtd as benchmark_return_mtd,
        bm.return_ytd as benchmark_return_ytd,
        bm.benchmark_name
    from latest_pnl lp
    left join DBT_DEMO.DEV.stg_benchmarks bm
        on lp.pnl_date = bm.benchmark_date
        and bm.benchmark_id = 'BM_SP500'
)

select
    portfolio_id,
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
    case
        when total_market_value != 0
        then position_daily_pnl / total_market_value
        else 0
    end as daily_return_pct
from pnl_with_benchmark
order by portfolio_id, pnl_date desc
  );

