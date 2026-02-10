
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fund_id
from "bain_analytics"."public"."report_lp_quarterly"
where fund_id is null



  
  
      
    ) dbt_internal_test