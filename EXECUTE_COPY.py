#!/usr/bin/env python3
"""Execute the copy of TRANSLATE files"""

import os
import shutil

source_dir = 'snowflake'
target_dir = 'snowflake4'

translate_files = [
    'macros/date_utils.sql',
    'macros/financial_calculations.sql',
    'models/staging/stg_benchmarks.sql',
    'models/staging/stg_cashflows.sql',
    'models/staging/stg_counterparties.sql',
    'models/staging/stg_dates.sql',
    'models/staging/stg_fund_structures.sql',
    'models/staging/stg_instruments.sql',
    'models/staging/stg_portfolios.sql',
    'models/staging/stg_positions.sql',
    'models/staging/stg_trades.sql',
    'models/staging/stg_valuations.sql',
    'models/intermediate/int_benchmark_returns.sql',
    'models/intermediate/int_cashflow_enriched.sql',
    'models/intermediate/int_daily_positions.sql',
    'models/intermediate/int_fund_nav.sql',
    'models/intermediate/int_irr_calculations.sql',
    'models/intermediate/int_portfolio_attribution.sql',
    'models/intermediate/int_trade_enriched.sql',
    'models/intermediate/int_valuation_enriched.sql',
    'models/marts/fact_cashflow_waterfall.sql',
    'models/marts/fact_fund_performance.sql',
    'models/marts/fact_portfolio_attribution.sql',
    'models/marts/fact_portfolio_pnl.sql',
    'models/marts/fact_portfolio_summary.sql',
    'models/marts/fact_trade_activity.sql',
    'models/marts/report_daily_pnl.sql',
    'models/marts/report_ic_dashboard.sql',
    'models/marts/report_lp_quarterly.sql',
    'models/marts/report_portfolio_overview.sql',
    'profiles.yml',
    'dbt_project.yml',
    'docker-compose.yml'
]

print(f"Copying {len(translate_files)} TRANSLATE files...")

for filepath in translate_files:
    src = os.path.join(source_dir, filepath)
    dst = os.path.join(target_dir, filepath)
    
    # Create directory
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    
    # Copy file
    if os.path.exists(src):
        shutil.copy2(src, dst)
        print(f"✓ {filepath}")
    else:
        print(f"✗ {filepath} (not found in source)")

print("Done!")
