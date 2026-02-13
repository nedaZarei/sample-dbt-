|-- Pipeline A: FACT table - Portfolio summary with positions
|-- Anti-pattern: unnecessary DISTINCT on already-unique aggregation
|-- Anti-pattern: late filtering (filters at the end instead of upstream)
|-- Anti-pattern: materialized as view (should be table)
|
|select distinct
|    p.portfolio_id,
|    p.portfolio_name,
|    p.strategy,
|    p.fund_id,
|    p.manager_name,
|    pos.position_date,
|    count(distinct pos.instrument_id) as num_positions,
|    sum(pos.market_value) as total_market_value,
|    sum(pos.cost_basis) as total_cost_basis,
|    sum(pos.unrealized_gain_loss) as total_unrealized_pnl,
|    case
|        when sum(pos.cost_basis) != 0
|        then sum(pos.unrealized_gain_loss) / abs(sum(pos.cost_basis))
|        else 0
|    end as portfolio_return_pct,
|    max(pos.market_value) as largest_position_mv,
|    min(pos.market_value) as smallest_position_mv
|from {{ ref('stg_portfolios') }} p
|inner join {{ ref('stg_positions') }} pos
|    on p.portfolio_id = pos.portfolio_id
|-- Anti-pattern: filter at the end instead of in staging
|where p.is_active = true
|group by
|    p.portfolio_id,
|    p.portfolio_name,
|    p.strategy,
|    p.fund_id,
|    p.manager_name,
|    pos.position_date
