
    
    

select
    fund_id as unique_field,
    count(*) as n_records

from "bain_analytics"."public"."stg_fund_structures"
where fund_id is not null
group by fund_id
having count(*) > 1


