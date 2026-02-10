-- Pipeline C: Enriched valuation data

with valuation_ranked as (
    select
        v.*,
        p.portfolio_name,
        p.strategy,
        p.fund_id,
        p.manager_name
    from DBT_DEMO.DEV.stg_valuations v
    inner join DBT_DEMO.DEV.stg_portfolios p
        on v.portfolio_id = p.portfolio_id
    qualify row_number() over (
        partition by v.portfolio_id, v.valuation_date
        order by v.valuation_id desc
    ) = 1
),

valuation_with_changes as (
    select
        vr.*,
        lag(vr.net_asset_value) over (
            partition by vr.portfolio_id order by vr.valuation_date
        ) as prev_nav,
        case
            when lag(vr.net_asset_value) over (
                partition by vr.portfolio_id order by vr.valuation_date
            ) is not null
            and lag(vr.net_asset_value) over (
                partition by vr.portfolio_id order by vr.valuation_date
            ) != 0
            then (vr.net_asset_value - lag(vr.net_asset_value) over (
                partition by vr.portfolio_id order by vr.valuation_date
            )) / lag(vr.net_asset_value) over (
                partition by vr.portfolio_id order by vr.valuation_date
            )
            else null
        end as nav_return_pct,
        datediff('day', lag(vr.valuation_date) over (
            partition by vr.portfolio_id order by vr.valuation_date
        ), vr.valuation_date) as days_between_valuations
    from valuation_ranked vr
)

select * from valuation_with_changes