-- Anti-pattern: unnecessary DISTINCT
select distinct
    valuation_id,
    portfolio_id,
    cast(valuation_date as date) as valuation_date,
    cast(gross_asset_value as numeric(18,2)) as gross_asset_value,
    cast(net_asset_value as numeric(18,2)) as net_asset_value,
    cast(total_liabilities as numeric(18,2)) as total_liabilities,
    cast(unrealized_pnl as numeric(18,2)) as unrealized_pnl,
    cast(realized_pnl as numeric(18,2)) as realized_pnl,
    valuation_method,
    -- Anti-pattern: duplicated fiscal quarter logic
    case
        when extract(month from cast(valuation_date as date)) between 1 and 3 then 'Q3'
        when extract(month from cast(valuation_date as date)) between 4 and 6 then 'Q4'
        when extract(month from cast(valuation_date as date)) between 7 and 9 then 'Q1'
        when extract(month from cast(valuation_date as date)) between 10 and 12 then 'Q2'
    end as valuation_fiscal_quarter
from DBT_DEMO.DEV_raw.raw_valuations