select distinct
    position_id,
    portfolio_id,
    instrument_id,
    cast(position_date as date) as position_date,
    cast(quantity as numeric(18,4)) as quantity,
    cast(market_value as numeric(18,2)) as market_value,
    cast(cost_basis as numeric(18,2)) as cost_basis,
    currency,
    cast(market_value as numeric(18,2)) - cast(cost_basis as numeric(18,2)) as unrealized_gain_loss,
    case
        when cast(cost_basis as numeric(18,2)) != 0
        then (cast(market_value as numeric(18,2)) - cast(cost_basis as numeric(18,2))) / abs(cast(cost_basis as numeric(18,2)))
        else 0
    end as unrealized_return_pct
from DBT_DEMO.DEV_raw.raw_positions