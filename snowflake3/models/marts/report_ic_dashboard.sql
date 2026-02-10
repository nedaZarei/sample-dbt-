-- Pipeline C: REPORT table - Investment Committee dashboard
-- Anti-pattern: massive query joining many sources, no intermediate reuse
-- Anti-pattern: subquery for latest records instead of QUALIFY
-- Anti-pattern: duplicated logic from other reports

with latest_fund_performance as (
    select *
    from (
        select
            fp.*,
            -- Anti-pattern: subquery dedup instead of QUALIFY
            row_number() over (
                partition by fp.fund_id
                order by fp.valuation_date desc
            ) as rn
        from {{ ref('fact_fund_performance') }} fp
    ) sub
    where rn = 1
),

latest_portfolio_summary as (
    select *
    from (
        select
            ps.*,
            row_number() over (
                partition by ps.portfolio_id
                order by ps.position_date desc
            ) as rn
        from {{ ref('fact_portfolio_summary') }} ps
    ) sub
    where rn = 1
),

-- Anti-pattern: re-joining fund structures (already in fact_fund_performance)
fund_overview as (
    select
        lfp.fund_id,
        lfp.fund_name,
        lfp.fund_type,
        lfp.committed_capital,
        lfp.valuation_date as latest_valuation_date,
        lfp.fund_nav,
        lfp.fund_gav,
        lfp.fund_nav_return as latest_nav_return,
        lfp.tvpi_gross,
        lfp.fund_approx_irr,
        lfp.fund_tvpi,
        lfp.fund_dpi,
        lfp.num_portfolios,
        lfp.fund_total_invested,
        lfp.fund_total_distributed,
        -- Anti-pattern: re-joining fund structures
        fs.management_fee_rate,
        fs.carry_rate,
        fs.hurdle_rate,
        fs.vintage_year
    from latest_fund_performance lfp
    inner join {{ ref('stg_fund_structures') }} fs
        on lfp.fund_id = fs.fund_id
),

portfolio_details as (
    select
        lps.portfolio_id,
        lps.portfolio_name,
        lps.strategy,
        lps.fund_id,
        lps.total_market_value,
        lps.total_unrealized_pnl,
        lps.portfolio_return_pct,
        lps.num_positions,
        -- Anti-pattern: re-computing fiscal quarter
        case
            when extract(month from lps.position_date) between 1 and 3 then 'Q3'
            when extract(month from lps.position_date) between 4 and 6 then 'Q4'
            when extract(month from lps.position_date) between 7 and 9 then 'Q1'
            when extract(month from lps.position_date) between 10 and 12 then 'Q2'
        end as reporting_fiscal_quarter
    from latest_portfolio_summary lps
)

select
    fo.fund_id,
    fo.fund_name,
    fo.fund_type,
    fo.vintage_year,
    fo.committed_capital,
    fo.latest_valuation_date,
    fo.fund_nav,
    fo.fund_gav,
    fo.latest_nav_return,
    fo.tvpi_gross,
    fo.fund_approx_irr,
    fo.fund_tvpi,
    fo.fund_dpi,
    fo.num_portfolios,
    fo.fund_total_invested,
    fo.fund_total_distributed,
    fo.management_fee_rate,
    fo.carry_rate,
    fo.hurdle_rate,
    pd.portfolio_id,
    pd.portfolio_name,
    pd.strategy,
    pd.total_market_value as portfolio_mv,
    pd.total_unrealized_pnl as portfolio_unrealized_pnl,
    pd.portfolio_return_pct,
    pd.num_positions as portfolio_num_positions,
    pd.reporting_fiscal_quarter
from fund_overview fo
inner join portfolio_details pd
    on fo.fund_id = pd.fund_id
order by fo.fund_nav desc, pd.total_market_value desc
