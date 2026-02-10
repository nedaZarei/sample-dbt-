
    
    

select
    trade_id as unique_field,
    count(*) as n_records

from "bain_analytics"."public"."int_trade_enriched"
where trade_id is not null
group by trade_id
having count(*) > 1


