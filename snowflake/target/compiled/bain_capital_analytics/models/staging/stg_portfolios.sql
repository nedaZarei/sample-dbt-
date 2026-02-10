select distinct
    cast(portfolio_id as varchar(10)) as portfolio_id,
    cast(portfolio_name as varchar(100)) as portfolio_name,
    cast(strategy as varchar(50)) as strategy,
    cast(inception_date as date) as inception_date,
    cast(fund_id as varchar(10)) as fund_id,
    cast(manager_name as varchar(100)) as manager_name,
    cast(is_active as boolean) as is_active,
    case
        when extract(month from cast(inception_date as date)) between 1 and 3 then 'Q3'
        when extract(month from cast(inception_date as date)) between 4 and 6 then 'Q4'
        when extract(month from cast(inception_date as date)) between 7 and 9 then 'Q1'
        when extract(month from cast(inception_date as date)) between 10 and 12 then 'Q2'
    end as inception_fiscal_quarter
from DBT_DEMO.DEV_raw.raw_portfolios
where cast(is_active as boolean) = true