-- Pipeline C: REPORT table - Investment Committee dashboard

with latest_fund_performance as (
    select
        fp.fund_id,
        fp.fund_name,
        fp.fund_type,
        fp.committed_capital,
        fp.valuation_date,
        fp.fund_nav,
        fp.fund_gav,
        fp.fund_nav_return,
        fp.tvpi_gross,
        fp.fund_approx_irr,
        fp.fund_tvpi,
        fp.fund_dpi,
        fp.num_portfolios,
        fp.fund_total_invested,
        fp.fund_total_distributed
    from {{ ref('fact_fund_performance') }} fp
    qualify row_number() over (
        partition by fp.fund_id
        order by fp.valuation_date desc
    ) = 1
),

latest_portfolio_summary as (
    select
        ps.portfolio_id,
        ps.portfolio_name,
        ps.strategy,
        ps.fund_id,
        ps.position_date,
        ps.total_market_value,
        ps.total_unrealized_pnl,
        ps.portfolio_return_pct,
        ps.num_positions
    from {{ ref('fact_portfolio_summary') }} ps
    qualify row_number() over (
        partition by ps.portfolio_id
        order by ps.position_date desc
    ) = 1
),

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
