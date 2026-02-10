-- Pipeline C: REPORT table - LP quarterly reporting
-- Anti-pattern: massive query, duplicated logic, no reuse of intermediates
-- Anti-pattern: PostgreSQL-specific date_trunc
-- Anti-pattern: re-aggregation of data already aggregated upstream

with quarterly_cashflows as (
    -- Anti-pattern: re-aggregating cashflows by quarter (already in int_cashflow_enriched)
    select
        cf.fund_id,
        cf.portfolio_id,
        -- Anti-pattern: PostgreSQL-specific date_trunc
        DATE_TRUNC('QUARTER', cf.cashflow_date) as quarter_start,
        cf.cashflow_fiscal_quarter as fiscal_quarter,
        cf.cashflow_year,
        sum(case when cf.cashflow_type = 'capital_call' then cf.amount else 0 end) as quarter_calls,
        sum(case when cf.cashflow_type = 'distribution' then cf.amount else 0 end) as quarter_distributions,
        sum(case when cf.cashflow_type = 'management_fee' then cf.amount else 0 end) as quarter_fees,
        sum(cf.amount) as quarter_net
    from {{ ref('stg_cashflows') }} cf
    group by
        cf.fund_id, cf.portfolio_id,
        DATE_TRUNC('QUARTER', cf.cashflow_date),
        cf.cashflow_fiscal_quarter, cf.cashflow_year
),

quarterly_valuations as (
    -- Anti-pattern: re-querying valuations with subquery for quarter-end
    select *
    from (
        select
            v.portfolio_id,
            v.valuation_date,
            v.net_asset_value,
            v.gross_asset_value,
            v.unrealized_pnl,
            v.realized_pnl,
            v.valuation_method,
            -- Anti-pattern: subquery dedup instead of QUALIFY
            row_number() over (
                partition by v.portfolio_id, DATE_TRUNC('QUARTER', v.valuation_date)
                order by v.valuation_date desc
            ) as rn
        from {{ ref('stg_valuations') }} v
    ) sub
    where rn = 1
),

-- Anti-pattern: re-joining fund structures (done in multiple places)
fund_details as (
    select
        fs.fund_id,
        fs.fund_name,
        fs.fund_type,
        fs.committed_capital,
        fs.management_fee_rate,
        fs.carry_rate,
        fs.hurdle_rate,
        fs.vintage_year,
        fs.gp_commitment_pct
    from {{ ref('stg_fund_structures') }} fs
),

-- Anti-pattern: joining everything at the end
lp_report as (
    select
        fd.fund_id,
        fd.fund_name,
        fd.fund_type,
        fd.vintage_year,
        fd.committed_capital,
        fd.management_fee_rate,
        fd.carry_rate,
        fd.hurdle_rate,
        fd.gp_commitment_pct,
        qc.portfolio_id,
        p.portfolio_name,
        p.strategy,
        qc.quarter_start,
        qc.fiscal_quarter,
        qc.cashflow_year,
        qc.quarter_calls,
        qc.quarter_distributions,
        qc.quarter_fees,
        qc.quarter_net,
        qv.net_asset_value as quarter_end_nav,
        qv.gross_asset_value as quarter_end_gav,
        qv.unrealized_pnl,
        qv.realized_pnl,
        qv.valuation_method,
        -- Anti-pattern: IRR re-joined from separate calculation
        irr.approx_irr,
        irr.tvpi,
        irr.dpi,
        irr.rvpi
    from quarterly_cashflows qc
    inner join fund_details fd
        on qc.fund_id = fd.fund_id
    inner join {{ ref('stg_portfolios') }} p
        on qc.portfolio_id = p.portfolio_id
    left join quarterly_valuations qv
        on qc.portfolio_id = qv.portfolio_id
        and DATE_TRUNC('QUARTER', qv.valuation_date) = qc.quarter_start
    left join {{ ref('int_irr_calculations') }} irr
        on qc.fund_id = irr.fund_id
        and qc.portfolio_id = irr.portfolio_id
)

select
    *,
    -- Anti-pattern: computed columns in final select
    case
        when committed_capital > 0
        then quarter_calls / committed_capital
        else 0
    end as quarterly_drawdown_rate,
    case
        when quarter_end_nav is not null and committed_capital > 0
        then quarter_end_nav / committed_capital
        else null
    end as nav_to_commitment_ratio
from lp_report
order by fund_id, portfolio_id, quarter_start
