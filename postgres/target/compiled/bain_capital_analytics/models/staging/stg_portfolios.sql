-- Anti-pattern: unnecessary DISTINCT when source data is already unique
-- Anti-pattern: casting in every model instead of once upstream

select distinct
    cast(portfolio_id as varchar(10)) as portfolio_id,
    cast(portfolio_name as varchar(100)) as portfolio_name,
    cast(strategy as varchar(50)) as strategy,
    cast(inception_date as date) as inception_date,
    cast(fund_id as varchar(10)) as fund_id,
    cast(manager_name as varchar(100)) as manager_name,
    cast(is_active as boolean) as is_active,
    -- Anti-pattern: duplicated fiscal quarter logic (repeated in many models)
    case
        when extract(month from cast(inception_date as date)) between 1 and 3 then 'Q3'
        when extract(month from cast(inception_date as date)) between 4 and 6 then 'Q4'
        when extract(month from cast(inception_date as date)) between 7 and 9 then 'Q1'
        when extract(month from cast(inception_date as date)) between 10 and 12 then 'Q2'
    end as inception_fiscal_quarter
from "bain_analytics"."public_raw"."raw_portfolios"
where cast(is_active as boolean) = true