select distinct
    counterparty_id,
    counterparty_name,
    counterparty_type,
    country,
    credit_rating,
    cast(is_active as boolean) as is_active
from {{ ref('raw_counterparties') }}
where cast(is_active as boolean) = TRUE
