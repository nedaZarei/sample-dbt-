
  create view "bain_analytics"."public"."stg_counterparties__dbt_tmp"
    
    
  as (
    select distinct
    counterparty_id,
    counterparty_name,
    counterparty_type,
    country,
    credit_rating,
    cast(is_active as boolean) as is_active
from "bain_analytics"."public_raw"."raw_counterparties"
where cast(is_active as boolean) = true
  );