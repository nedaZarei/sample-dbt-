-- Anti-pattern: unnecessary DISTINCT
select distinct
    instrument_id,
    instrument_name,
    instrument_type,
    sector,
    currency,
    exchange,
    issuer,
    cast(maturity_date as date) as maturity_date,
    -- Anti-pattern: PostgreSQL-specific date subtraction
    case
        when maturity_date is not null
        then datediff(day, current_date, cast(maturity_date as date))
        else null
    end as days_to_maturity,
    case
        when instrument_type in ('equity', 'futures') then 'liquid'
        when instrument_type in ('corporate_bond', 'government_bond', 'sovereign_bond', 'leveraged_loan') then 'semi_liquid'
        when instrument_type in ('private_equity', 'venture_capital', 'real_estate', 'real_estate_debt', 'infrastructure') then 'illiquid'
        else 'other'
    end as liquidity_class
from DBT_DEMO.DEV_raw.raw_instruments