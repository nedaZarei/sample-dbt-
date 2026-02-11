#!/usr/bin/env python3
"""
Copy TRANSLATE Files from snowflake/ to snowflake4/

After verification that all TRANSLATE files are correct (only pattern-compliant changes),
this script copies all 33 verified files from snowflake/ to snowflake4/.
"""

import os
import shutil
import hashlib
from pathlib import Path


def compute_file_hash(filepath):
    """Compute SHA256 hash of a file"""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for block in iter(lambda: f.read(4096), b''):
            sha256.update(block)
    return sha256.hexdigest()


def copy_translate_files():
    """Copy all 33 TRANSLATE files from snowflake/ to snowflake4/"""
    
    source_dir = 'snowflake'
    target_dir = 'snowflake4'
    
    # Define all 33 TRANSLATE files
    translate_files = [
        # Macro files (2)
        'macros/date_utils.sql',
        'macros/financial_calculations.sql',
        
        # Staging models (10)
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
        
        # Intermediate models (8)
        'models/intermediate/int_benchmark_returns.sql',
        'models/intermediate/int_cashflow_enriched.sql',
        'models/intermediate/int_daily_positions.sql',
        'models/intermediate/int_fund_nav.sql',
        'models/intermediate/int_irr_calculations.sql',
        'models/intermediate/int_portfolio_attribution.sql',
        'models/intermediate/int_trade_enriched.sql',
        'models/intermediate/int_valuation_enriched.sql',
        
        # Marts models (10)
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
        
        # Config files (3)
        'profiles.yml',
        'dbt_project.yml',
        'docker-compose.yml'
    ]
    
    print("=" * 80)
    print("COPYING TRANSLATE FILES FROM snowflake/ TO snowflake4/")
    print("=" * 80)
    print()
    
    total_files = len(translate_files)
    copied_files = 0
    skipped_files = 0
    failed_files = 0
    
    file_hashes = []
    
    for filepath in translate_files:
        source_path = os.path.join(source_dir, filepath)
        target_path = os.path.join(target_dir, filepath)
        
        # Create directories if needed
        target_dir_path = os.path.dirname(target_path)
        if not os.path.exists(target_dir_path):
            os.makedirs(target_dir_path)
        
        # Check if source exists
        if not os.path.exists(source_path):
            print(f"⚠️  SKIPPED: {filepath}")
            print(f"   Reason: Source file not found")
            skipped_files += 1
            continue
        
        # Copy file
        try:
            shutil.copy2(source_path, target_path)
            
            # Verify copy
            source_hash = compute_file_hash(source_path)
            target_hash = compute_file_hash(target_path)
            
            if source_hash == target_hash:
                print(f"✅ COPIED: {filepath}")
                print(f"   Hash: {source_hash[:16]}...")
                copied_files += 1
                file_hashes.append({
                    'file': filepath,
                    'source_hash': source_hash,
                    'target_hash': target_hash,
                    'status': 'OK'
                })
            else:
                print(f"❌ FAILED: {filepath}")
                print(f"   Reason: Hash mismatch after copy")
                failed_files += 1
                file_hashes.append({
                    'file': filepath,
                    'source_hash': source_hash,
                    'target_hash': target_hash,
                    'status': 'HASH_MISMATCH'
                })
        except Exception as e:
            print(f"❌ FAILED: {filepath}")
            print(f"   Reason: {str(e)}")
            failed_files += 1
            file_hashes.append({
                'file': filepath,
                'status': 'ERROR',
                'error': str(e)
            })
        
        print()
    
    # Summary
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total Files:       {total_files}")
    print(f"Successfully Copied: {copied_files}")
    print(f"Skipped:           {skipped_files}")
    print(f"Failed:            {failed_files}")
    print()
    
    if failed_files == 0 and skipped_files == 0:
        print("✅ All TRANSLATE files copied successfully!")
        return 0
    else:
        print("⚠️  Some files were not copied successfully")
        return 1


if __name__ == '__main__':
    exit(copy_translate_files())
