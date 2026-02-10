
    
    

select
    portfolio_id as unique_field,
    count(*) as n_records

from "bain_analytics"."public"."stg_portfolios"
where portfolio_id is not null
group by portfolio_id
having count(*) > 1


