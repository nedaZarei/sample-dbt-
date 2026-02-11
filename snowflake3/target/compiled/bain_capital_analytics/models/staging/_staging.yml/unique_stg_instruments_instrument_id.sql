
    
    

select
    instrument_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.stg_instruments
where instrument_id is not null
group by instrument_id
having count(*) > 1


