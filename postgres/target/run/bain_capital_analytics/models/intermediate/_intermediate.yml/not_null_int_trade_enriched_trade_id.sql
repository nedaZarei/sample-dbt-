
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select trade_id
from "bain_analytics"."public"."int_trade_enriched"
where trade_id is null



  
  
      
    ) dbt_internal_test