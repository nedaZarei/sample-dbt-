
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    valuation_id as unique_field,
    count(*) as n_records

from "bain_analytics"."public"."stg_valuations"
where valuation_id is not null
group by valuation_id
having count(*) > 1



  
  
      
    ) dbt_internal_test