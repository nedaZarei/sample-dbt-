
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select portfolio_id as from_field
    from DBT_DEMO.DEV.stg_positions
    where portfolio_id is not null
),

parent as (
    select portfolio_id as to_field
    from DBT_DEMO.DEV.stg_portfolios
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test