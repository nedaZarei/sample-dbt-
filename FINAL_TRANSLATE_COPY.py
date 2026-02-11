#!/usr/bin/env python3
"""
Final TRANSLATE Files Copy and Verification
Copies all 33 verified TRANSLATE files from snowflake/ to snowflake4/
"""

import os
import shutil
import hashlib
import json
from datetime import datetime


def ensure_dir(path):
    """Create directory if it doesn't exist"""
    os.makedirs(path, exist_ok=True)


def copy_file_with_verify(src, dst):
    """Copy file and verify"""
    # Create target directory
    ensure_dir(os.path.dirname(dst))
    
    # Copy file
    shutil.copy2(src, dst)
    
    # Verify
    with open(src, 'rb') as f:
        src_content = f.read()
    with open(dst, 'rb') as f:
        dst_content = f.read()
    
    return src_content == dst_content


def get_file_hash(filepath):
    """Get SHA256 hash of file"""
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        for block in iter(lambda: f.read(4096), b''):
            sha256.update(block)
    return sha256.hexdigest()


def main():
    source_dir = 'snowflake'
    target_dir = 'snowflake4'
    
    # All 33 TRANSLATE files
    files_to_copy = [
        # Macros (2)
        'macros/date_utils.sql',
        'macros/financial_calculations.sql',
        
        # Staging (10)
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
        
        # Intermediate (8)
        'models/intermediate/int_benchmark_returns.sql',
        'models/intermediate/int_cashflow_enriched.sql',
        'models/intermediate/int_daily_positions.sql',
        'models/intermediate/int_fund_nav.sql',
        'models/intermediate/int_irr_calculations.sql',
        'models/intermediate/int_portfolio_attribution.sql',
        'models/intermediate/int_trade_enriched.sql',
        'models/intermediate/int_valuation_enriched.sql',
        
        # Marts (10)
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
        
        # Config (3)
        'profiles.yml',
        'dbt_project.yml',
        'docker-compose.yml'
    ]
    
    print("\n" + "="*80)
    print("COPYING TRANSLATE FILES FROM snowflake/ TO snowflake4/")
    print("="*80 + "\n")
    
    results = {
        'timestamp': datetime.now().isoformat(),
        'source_dir': source_dir,
        'target_dir': target_dir,
        'total_files': len(files_to_copy),
        'copied': [],
        'failed': [],
        'skipped': []
    }
    
    for i, filepath in enumerate(files_to_copy, 1):
        src_path = os.path.join(source_dir, filepath)
        dst_path = os.path.join(target_dir, filepath)
        
        # Check if source exists
        if not os.path.exists(src_path):
            print(f"[{i:2d}/{len(files_to_copy)}] ⚠️  SKIPPED: {filepath}")
            results['skipped'].append(filepath)
            continue
        
        try:
            # Copy with verification
            if copy_file_with_verify(src_path, dst_path):
                src_hash = get_file_hash(src_path)
                dst_hash = get_file_hash(dst_path)
                print(f"[{i:2d}/{len(files_to_copy)}] ✅ COPIED: {filepath}")
                results['copied'].append({
                    'file': filepath,
                    'source_hash': src_hash,
                    'target_hash': dst_hash
                })
            else:
                print(f"[{i:2d}/{len(files_to_copy)}] ❌ FAILED: {filepath} (verification failed)")
                results['failed'].append({
                    'file': filepath,
                    'reason': 'Verification failed - content mismatch'
                })
        except Exception as e:
            print(f"[{i:2d}/{len(files_to_copy)}] ❌ FAILED: {filepath} ({str(e)})")
            results['failed'].append({
                'file': filepath,
                'reason': str(e)
            })
    
    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"\nTotal Files:         {len(files_to_copy)}")
    print(f"Successfully Copied: {len(results['copied'])} ({len(results['copied'])/len(files_to_copy)*100:.1f}%)")
    print(f"Skipped:             {len(results['skipped'])}")
    print(f"Failed:              {len(results['failed'])}")
    
    if results['failed']:
        print(f"\nFailed files:")
        for item in results['failed']:
            print(f"  - {item['file']}: {item['reason']}")
    
    if results['skipped']:
        print(f"\nSkipped files:")
        for item in results['skipped']:
            print(f"  - {item}")
    
    print()
    
    # Save results
    with open('TRANSLATE_COPY_RESULTS.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("Results saved to TRANSLATE_COPY_RESULTS.json\n")
    
    if len(results['failed']) == 0:
        print("✅ All TRANSLATE files copied successfully!")
        return 0
    else:
        print(f"⚠️  {len(results['failed'])} file(s) failed to copy")
        return 1


if __name__ == '__main__':
    exit(main())
