
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    position_id as unique_field,
    count(*) as n_records

from "bain_analytics"."public"."stg_positions"
where position_id is not null
group by position_id
having count(*) > 1



  
  
      
    ) dbt_internal_test