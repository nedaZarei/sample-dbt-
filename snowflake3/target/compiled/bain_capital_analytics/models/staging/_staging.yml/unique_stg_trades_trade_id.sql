
    
    

select
    trade_id as unique_field,
    count(*) as n_records

from DBT_DEMO.DEV.stg_trades
where trade_id is not null
group by trade_id
having count(*) > 1


