
    
    

select
    cashflow_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.stg_cashflows
where cashflow_id is not null
group by cashflow_id
having count(*) > 1


