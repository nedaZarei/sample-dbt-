
  create or replace   view DBT_DEMO.DEV.int_portfolio_attribution
  
  
  
  
  as (
    -- Pipeline C: Portfolio performance attribution by sector and instrument type

with positions_enriched as (
    select
        pos.position_id,
        pos.portfolio_id,
        pos.instrument_id,
        pos.position_date,
        pos.quantity,
        pos.market_value,
        pos.cost_basis,
        pos.unrealized_gain_loss,
        i.instrument_name,
        i.instrument_type,
        i.sector,
        p.portfolio_name,
        p.strategy,
        p.fund_id
    from DBT_DEMO.DEV.stg_positions pos
    inner join DBT_DEMO.DEV.stg_instruments i
        on pos.instrument_id = i.instrument_id
    inner join DBT_DEMO.DEV.stg_portfolios p
        on pos.portfolio_id = p.portfolio_id
),

portfolio_totals as (
    select
        portfolio_id,
        position_date,
        sum(market_value) as total_portfolio_mv,
        sum(unrealized_gain_loss) as total_portfolio_ugl
    from positions_enriched
    group by portfolio_id, position_date
),

sector_attribution as (
    select
        pe.portfolio_id,
        pe.portfolio_name,
        pe.strategy,
        pe.fund_id,
        pe.position_date,
        pe.sector,
        sum(pe.market_value) as sector_mv,
        sum(pe.unrealized_gain_loss) as sector_ugl,
        count(distinct pe.instrument_id) as num_instruments
    from positions_enriched pe
    group by
        pe.portfolio_id, pe.portfolio_name, pe.strategy,
        pe.fund_id, pe.position_date, pe.sector
),

attribution_with_weights as (
    select
        sa.*,
        pt.total_portfolio_mv,
        pt.total_portfolio_ugl,
        case
            when pt.total_portfolio_mv != 0
            then sa.sector_mv / pt.total_portfolio_mv
            else 0
        end as sector_weight,
        case
            when pt.total_portfolio_ugl != 0
            then sa.sector_ugl / pt.total_portfolio_ugl
            else 0
        end as sector_contribution
    from sector_attribution sa
    inner join portfolio_totals pt
        on sa.portfolio_id = pt.portfolio_id
        and sa.position_date = pt.position_date
)

select * from attribution_with_weights
  );

