
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    cashflow_id as unique_field,
    count(*) as n_records

from "bain_analytics"."public"."stg_cashflows"
where cashflow_id is not null
group by cashflow_id
having count(*) > 1



  
  
      
    ) dbt_internal_test