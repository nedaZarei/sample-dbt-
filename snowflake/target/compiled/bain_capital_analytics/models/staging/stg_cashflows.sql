select distinct
    cashflow_id,
    fund_id,
    portfolio_id,
    cast(cashflow_date as date) as cashflow_date,
    cashflow_type,
    cast(amount as numeric(18,2)) as amount,
    currency,
    description,
    investor_id,
    case
        when extract(month from cast(cashflow_date as date)) between 1 and 3 then 'Q3'
        when extract(month from cast(cashflow_date as date)) between 4 and 6 then 'Q4'
        when extract(month from cast(cashflow_date as date)) between 7 and 9 then 'Q1'
        when extract(month from cast(cashflow_date as date)) between 10 and 12 then 'Q2'
    end as cashflow_fiscal_quarter,
    extract(year from cast(cashflow_date as date)) as cashflow_year
from DBT_DEMO.DEV_raw.raw_cashflows