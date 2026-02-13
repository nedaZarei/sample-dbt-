-- Anti-pattern: unnecessary DISTINCT on already-unique trade_id
-- Anti-pattern: heavy casting repeated everywhere

select distinct
    trade_id,
    portfolio_id,
    instrument_id,
    counterparty_id,
    cast(trade_date as date) as trade_date,
    cast(settlement_date as date) as settlement_date,
    trade_type,
    cast(quantity as number(18,4)) as quantity,
    cast(price as number(18,6)) as price,
    cast(notional_amount as number(18,2)) as notional_amount,
    cast(commission as number(18,2)) as commission,
    currency,
    status,
    -- Anti-pattern: duplicated fiscal quarter logic
    case
        when extract(month from cast(trade_date as date)) between 1 and 3 then 'Q3'
        when extract(month from cast(trade_date as date)) between 4 and 6 then 'Q4'
        when extract(month from cast(trade_date as date)) between 7 and 9 then 'Q1'
        when extract(month from cast(trade_date as date)) between 10 and 12 then 'Q2'
    end as trade_fiscal_quarter,
    -- Anti-pattern: PostgreSQL-specific date arithmetic
    datediff(day, cast(trade_date as date), cast(settlement_date as date)) as settlement_days
from {{ ref('raw_trades') }}
where status = 'settled'
