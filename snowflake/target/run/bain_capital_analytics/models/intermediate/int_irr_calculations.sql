
  create or replace   view DBT_DEMO.DEV.int_irr_calculations
  
  
  
  
  as (
    -- Pipeline C: Approximate IRR calculation per fund/portfolio

with cashflow_series as (
    -- CTE level 1: get all cashflows with proper signs
    select
        cf.fund_id,
        cf.portfolio_id,
        cf.cashflow_date,
        cf.cashflow_type,
        case
            when cf.cashflow_type = 'capital_call' then -1 * abs(cf.amount)
            when cf.cashflow_type = 'distribution' then abs(cf.amount)
            when cf.cashflow_type = 'management_fee' then -1 * abs(cf.amount)
            else cf.amount
        end as signed_amount
    from DBT_DEMO.DEV.stg_cashflows cf
),

terminal_values as (
    -- CTE level 2: get latest NAV as terminal cashflow
    select
        v.portfolio_id,
        p.fund_id,
        v.valuation_date as cashflow_date,
        'terminal_value' as cashflow_type,
        v.net_asset_value as signed_amount
    from DBT_DEMO.DEV.stg_valuations v
    inner join DBT_DEMO.DEV.stg_portfolios p
        on v.portfolio_id = p.portfolio_id
    where v.valuation_date = (
        select max(v2.valuation_date)
        from DBT_DEMO.DEV.stg_valuations v2
        where v2.portfolio_id = v.portfolio_id
    )
),

all_cashflows as (
    -- CTE level 3: union cashflows and terminal values
    select fund_id, portfolio_id, cashflow_date, cashflow_type, signed_amount
    from cashflow_series
    union all
    select fund_id, portfolio_id, cashflow_date, cashflow_type, signed_amount
    from terminal_values
),

cashflow_with_timing as (
    -- CTE level 4: compute time fractions
    select
        ac.*,
        min(ac.cashflow_date) over (partition by ac.fund_id, ac.portfolio_id) as first_cf_date,
        cast(datediff('day', min(ac.cashflow_date) over (partition by ac.fund_id, ac.portfolio_id), ac.cashflow_date) as number) / 365.25 as year_fraction
    from all_cashflows ac
),

-- CTE level 5: compute simple multiples as IRR proxy
portfolio_multiples as (
    select
        fund_id,
        portfolio_id,
        min(cashflow_date) as first_cashflow_date,
        max(cashflow_date) as last_cashflow_date,
        datediff('day', min(cashflow_date), max(cashflow_date)) as investment_days,
        cast(datediff('day', min(cashflow_date), max(cashflow_date)) as number) / 365.25 as investment_years,
        sum(case when cashflow_type = 'capital_call' then abs(signed_amount) else 0 end) as total_invested,
        sum(case when cashflow_type = 'distribution' then signed_amount else 0 end) as total_distributed,
        sum(case when cashflow_type = 'terminal_value' then signed_amount else 0 end) as terminal_value,
        sum(signed_amount) as net_cashflow
    from cashflow_with_timing
    group by fund_id, portfolio_id
),

-- CTE level 6: compute IRR approximation
irr_approximation as (
    select
        pm.*,
        case
            when pm.total_invested > 0
            then (pm.total_distributed + pm.terminal_value) / pm.total_invested
            else null
        end as tvpi,
        case
            when pm.total_invested > 0
            then pm.total_distributed / pm.total_invested
            else null
        end as dpi,
        case
            when pm.total_invested > 0
            then pm.terminal_value / pm.total_invested
            else null
        end as rvpi,
        -- Simplified IRR approximation: (TVPI^(1/years)) - 1
        case
            when pm.total_invested > 0 and pm.investment_years > 0
            then power(
                (pm.total_distributed + pm.terminal_value) / pm.total_invested,
                1.0 / pm.investment_years
            ) - 1
            else null
        end as approx_irr
    from portfolio_multiples pm
)

select * from irr_approximation
  );

