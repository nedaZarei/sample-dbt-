|-- Pipeline C: FACT table - Portfolio attribution with benchmark comparison
|-- Anti-pattern: re-joins data that's already in intermediate models
|-- Anti-pattern: repeated computation of sector weights
|
|with attribution as (
|    select
|        pa.portfolio_id,
|        pa.portfolio_name,
|        pa.strategy,
|        pa.fund_id,
|        pa.position_date,
|        pa.sector,
|        pa.sector_mv,
|        pa.sector_ugl,
|        pa.num_instruments,
|        pa.total_portfolio_mv,
|        pa.total_portfolio_ugl,
|        pa.sector_weight,
|        pa.sector_contribution
|    from {{ ref('int_portfolio_attribution') }} pa
|),
|
|-- Anti-pattern: re-joining benchmarks (could be done in intermediate)
|attribution_with_benchmark as (
|    select
|        a.*,
|        bm.benchmark_name,
|        bm.return_mtd as benchmark_return,
|        bm.rolling_3m_return as benchmark_3m_return,
|        bm.rolling_12m_return as benchmark_12m_return
|    from attribution a
|    left join {{ ref('int_benchmark_returns') }} bm
|        on a.position_date = bm.benchmark_date
|        and bm.benchmark_id = 'BM_SP500'
|),
|
|-- Anti-pattern: computing alpha in yet another CTE
|attribution_with_alpha as (
|    select
|        ab.*,
|        -- Simplified active return
|        case
|            when ab.total_portfolio_mv != 0
|            then (ab.sector_ugl / ab.total_portfolio_mv) - coalesce(ab.benchmark_return, 0) * ab.sector_weight
|            else 0
|        end as sector_active_return,
|        -- Anti-pattern: re-computing portfolio-level return here
|        case
|            when ab.total_portfolio_mv != 0
|            then ab.total_portfolio_ugl / ab.total_portfolio_mv
|            else 0
|        end as portfolio_return
|    from attribution_with_benchmark ab
|)
|
|select * from attribution_with_alpha
