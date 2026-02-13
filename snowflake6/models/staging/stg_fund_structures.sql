select distinct
    fund_id,
    fund_name,
    fund_type,
    cast(vintage_year as integer) as vintage_year,
    cast(committed_capital as number(18,2)) as committed_capital,
    cast(management_fee_rate as number(8,4)) as management_fee_rate,
    cast(carry_rate as number(8,4)) as carry_rate,
    cast(hurdle_rate as number(8,4)) as hurdle_rate,
    cast(gp_commitment_pct as number(8,4)) as gp_commitment_pct,
    fund_currency,
    fund_status
from {{ ref('raw_fund_structures') }}
