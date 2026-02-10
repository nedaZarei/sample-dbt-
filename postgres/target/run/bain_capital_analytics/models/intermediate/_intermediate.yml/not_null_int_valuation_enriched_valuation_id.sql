
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select valuation_id
from "bain_analytics"."public"."int_valuation_enriched"
where valuation_id is null



  
  
      
    ) dbt_internal_test