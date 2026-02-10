
  create or replace   view DBT_DEMO.DEV.stg_counterparties
  
  
  
  
  as (
    select distinct
    counterparty_id,
    counterparty_name,
    counterparty_type,
    country,
    credit_rating,
    cast(is_active as boolean) as is_active
from DBT_DEMO.DEV_raw.raw_counterparties
where cast(is_active as boolean) = TRUE
  );

