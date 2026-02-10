
    
    

select
    valuation_id as unique_field,
    count(*) as n_records

from "bain_analytics"."public"."stg_valuations"
where valuation_id is not null
group by valuation_id
having count(*) > 1


