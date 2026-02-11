
    
    

select
    valuation_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.stg_valuations
where valuation_id is not null
group by valuation_id
having count(*) > 1


