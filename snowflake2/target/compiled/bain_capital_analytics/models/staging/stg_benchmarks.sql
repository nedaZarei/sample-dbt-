select distinct
    benchmark_id,
    benchmark_name,
    cast(benchmark_date as date) as benchmark_date,
    cast(return_daily as numeric(18,8)) as return_daily,
    cast(return_mtd as numeric(18,8)) as return_mtd,
    cast(return_ytd as numeric(18,8)) as return_ytd,
    cast(benchmark_level as numeric(18,4)) as benchmark_level,
    -- Anti-pattern: duplicated fiscal quarter logic
    case
        when extract(month from cast(benchmark_date as date)) between 1 and 3 then 'Q3'
        when extract(month from cast(benchmark_date as date)) between 4 and 6 then 'Q4'
        when extract(month from cast(benchmark_date as date)) between 7 and 9 then 'Q1'
        when extract(month from cast(benchmark_date as date)) between 10 and 12 then 'Q2'
    end as benchmark_fiscal_quarter
from DBT_DEMO.DEV_raw.raw_benchmarks