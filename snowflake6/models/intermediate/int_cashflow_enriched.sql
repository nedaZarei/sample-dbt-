|-- Pipeline C: Enriched cashflow data with fund and portfolio details
|-- Anti-pattern: subquery for dedup instead of QUALIFY
|-- Anti-pattern: joins repeated from other intermediate models
|
|with cashflow_with_details as (
|    select *
|    from (
|        select
|            cf.*,
|            p.portfolio_name,
|            p.strategy,
|            p.manager_name,
|            fs.fund_name,
|            fs.fund_type,
|            fs.committed_capital,
|            fs.management_fee_rate,
|            fs.carry_rate,
|            fs.hurdle_rate,
|            -- Anti-pattern: subquery filter instead of QUALIFY
|            row_number() over (
|                partition by cf.cashflow_id
|                order by cf.cashflow_date desc
|            ) as rn
|        from {{ ref('stg_cashflows') }} cf
|        inner join {{ ref('stg_portfolios') }} p
|            on cf.portfolio_id = p.portfolio_id
|        inner join {{ ref('stg_fund_structures') }} fs
|            on cf.fund_id = fs.fund_id
|    ) sub
|    where rn = 1
),
|
|-- Anti-pattern: running totals computed here AND again downstream
|cashflow_cumulative as (
|    select
|        *,
|        sum(case when cashflow_type = 'capital_call' then amount else 0 end) over (
|            partition by fund_id, portfolio_id
|            order by cashflow_date
|            rows between unbounded preceding and current row
|        ) as cumulative_called,
|        sum(case when cashflow_type = 'distribution' then amount else 0 end) over (
|            partition by fund_id, portfolio_id
|            order by cashflow_date
|            rows between unbounded preceding and current row
|        ) as cumulative_distributed,
|        sum(amount) over (
|            partition by fund_id, portfolio_id
|            order by cashflow_date
|            rows between unbounded preceding and current row
|        ) as cumulative_net_cashflow
|    from cashflow_with_details
|)
|
|select
|    cashflow_id,
|    fund_id,
|    fund_name,
|    fund_type,
|    portfolio_id,
|    portfolio_name,
|    strategy,
|    manager_name,
|    cashflow_date,
|    cashflow_type,
|    amount,
|    currency,
|    description,
|    investor_id,
|    cashflow_fiscal_quarter,
|    cashflow_year,
|    committed_capital,
|    management_fee_rate,
|    carry_rate,
|    hurdle_rate,
|    cumulative_called,
|    cumulative_distributed,
|    cumulative_net_cashflow,
|    case
|        when committed_capital > 0
|        then cumulative_called / committed_capital
|        else 0
|    end as pct_called
|from cashflow_cumulative
