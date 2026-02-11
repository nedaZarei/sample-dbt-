
  create or replace   view DBT_DEMO.DEV.int_benchmark_returns
  
  
  
  
  as (
    -- Pipeline C: Benchmark returns with rolling calculations
-- Anti-pattern: heavy window functions that could be pre-computed

with benchmark_base as (
    select
        b.benchmark_id,
        b.benchmark_name,
        b.benchmark_date,
        b.return_daily,
        b.return_mtd,
        b.return_ytd,
        b.benchmark_level,
        b.benchmark_fiscal_quarter,
        d.fiscal_year,
        d.is_quarter_end,
        d.is_year_end
    from DBT_DEMO.DEV.stg_benchmarks b
    inner join DBT_DEMO.DEV.stg_dates d
        on b.benchmark_date = d.date_day
),

-- Anti-pattern: compute many overlapping windows
benchmark_with_rolling as (
    select
        bb.*,
        -- rolling 3-month return
        exp(sum(ln(1 + bb.return_mtd)) over (
            partition by bb.benchmark_id
            order by bb.benchmark_date
            rows between 2 preceding and current row
        )) - 1 as rolling_3m_return,
        -- rolling 6-month return
        exp(sum(ln(1 + bb.return_mtd)) over (
            partition by bb.benchmark_id
            order by bb.benchmark_date
            rows between 5 preceding and current row
        )) - 1 as rolling_6m_return,
        -- rolling 12-month return
        exp(sum(ln(1 + bb.return_mtd)) over (
            partition by bb.benchmark_id
            order by bb.benchmark_date
            rows between 11 preceding and current row
        )) - 1 as rolling_12m_return,
        -- Anti-pattern: volatility via window (expensive)
        stddev(bb.return_mtd) over (
            partition by bb.benchmark_id
            order by bb.benchmark_date
            rows between 11 preceding and current row
        ) as rolling_12m_volatility,
        avg(bb.return_mtd) over (
            partition by bb.benchmark_id
            order by bb.benchmark_date
            rows between 11 preceding and current row
        ) as rolling_12m_avg_return,
        lag(bb.benchmark_level) over (
            partition by bb.benchmark_id
            order by bb.benchmark_date
        ) as prev_benchmark_level
    from benchmark_base bb
)

select * from benchmark_with_rolling
  );

