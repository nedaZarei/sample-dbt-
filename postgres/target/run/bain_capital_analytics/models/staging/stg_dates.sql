
  create view "bain_analytics"."public"."stg_dates__dbt_tmp"
    
    
  as (
    select
    cast(date_day as date) as date_day,
    cast(is_business_day as boolean) as is_business_day,
    cast(is_month_end as boolean) as is_month_end,
    cast(is_quarter_end as boolean) as is_quarter_end,
    cast(is_year_end as boolean) as is_year_end,
    fiscal_quarter,
    fiscal_year,
    -- Anti-pattern: PostgreSQL-specific date_trunc
    date_trunc('month', cast(date_day as date)) as month_start,
    date_trunc('quarter', cast(date_day as date)) as quarter_start,
    extract(month from cast(date_day as date)) as calendar_month,
    extract(year from cast(date_day as date)) as calendar_year
from "bain_analytics"."public_raw"."raw_dates"
  );