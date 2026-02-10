
    
    

select
    cashflow_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.int_cashflow_enriched
where cashflow_id is not null
group by cashflow_id
having count(*) > 1


