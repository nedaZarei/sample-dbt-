select
    cast(date_day as date) as date_day,
    cast(is_business_day as boolean) as is_business_day,
    cast(is_month_end as boolean) as is_month_end,
    cast(is_quarter_end as boolean) as is_quarter_end,
    cast(is_year_end as boolean) as is_year_end,
    fiscal_quarter,
    fiscal_year,
    -- Anti-pattern: PostgreSQL-specific date_trunc
    trunc(cast(date_day as date), 'month') as month_start,
    trunc(cast(date_day as date), 'quarter') as quarter_start,
    extract(month from cast(date_day as date)) as calendar_month,
    extract(year from cast(date_day as date)) as calendar_year
from {{ ref('raw_dates') }}
