
    
    

select
    counterparty_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.stg_counterparties
where counterparty_id is not null
group by counterparty_id
having count(*) > 1


