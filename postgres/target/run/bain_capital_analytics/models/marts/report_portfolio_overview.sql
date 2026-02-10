
  create view "bain_analytics"."public"."report_portfolio_overview__dbt_tmp"
    
    
  as (
    -- Pipeline A: REPORT table - Portfolio overview for management reporting
-- Anti-pattern: re-aggregates from fact instead of using fact directly in some cases
-- Anti-pattern: subquery instead of QUALIFY for latest date

select
    fps.portfolio_id,
    fps.portfolio_name,
    fps.strategy,
    fps.manager_name,
    fps.position_date as reporting_date,
    fps.num_positions,
    fps.total_market_value,
    fps.total_cost_basis,
    fps.total_unrealized_pnl,
    fps.portfolio_return_pct,
    fps.largest_position_mv,
    fps.smallest_position_mv,
    -- Anti-pattern: duplicated fiscal quarter logic AGAIN
    case
        when extract(month from fps.position_date) between 1 and 3 then 'Q3'
        when extract(month from fps.position_date) between 4 and 6 then 'Q4'
        when extract(month from fps.position_date) between 7 and 9 then 'Q1'
        when extract(month from fps.position_date) between 10 and 12 then 'Q2'
    end as fiscal_quarter,
    case
        when extract(month from fps.position_date) >= 7
        then extract(year from fps.position_date) + 1
        else extract(year from fps.position_date)
    end as fiscal_year
from "bain_analytics"."public"."fact_portfolio_summary" fps
where fps.position_date = (
    -- Anti-pattern: correlated subquery instead of window function / QUALIFY
    select max(fps2.position_date)
    from "bain_analytics"."public"."fact_portfolio_summary" fps2
    where fps2.portfolio_id = fps.portfolio_id
)
order by fps.total_market_value desc
  );