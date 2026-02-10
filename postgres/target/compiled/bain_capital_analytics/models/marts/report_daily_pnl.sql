-- Pipeline B: REPORT table - Daily PnL report for portfolio managers
-- Anti-pattern: subquery for latest-only instead of QUALIFY
-- Anti-pattern: re-aggregation from fact that's already aggregated

with latest_pnl as (
    select *
    from (
        select
            fpnl.*,
            -- Anti-pattern: subquery dedup instead of QUALIFY
            row_number() over (
                partition by fpnl.portfolio_id
                order by fpnl.pnl_date desc
            ) as rn
        from "bain_analytics"."public"."fact_portfolio_pnl" fpnl
    ) sub
    where rn <= 5  -- last 5 dates per portfolio
),

-- Anti-pattern: re-joining to get benchmark data that should be part of the pipeline
pnl_with_benchmark as (
    select
        lp.*,
        bm.return_mtd as benchmark_return_mtd,
        bm.return_ytd as benchmark_return_ytd,
        bm.benchmark_name
    from latest_pnl lp
    left join "bain_analytics"."public"."stg_benchmarks" bm
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
    -- Anti-pattern: computing return inline instead of upstream
    case
        when total_market_value != 0
        then position_daily_pnl / total_market_value
        else 0
    end as daily_return_pct
from pnl_with_benchmark
order by portfolio_id, pnl_date desc