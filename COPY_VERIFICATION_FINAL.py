#!/usr/bin/env python3
"""
FINAL COPY FILES VERIFICATION SCRIPT

Complete verification of all 18 COPY files:
- Copies remaining files (schemas directory files)
- Computes SHA256 checksums
- Compares source vs target
- Generates final verification report
"""

import os
import shutil
import hashlib
import json
from datetime import datetime
from pathlib import Path

SOURCE_DIR = './postgres'
TARGET_DIR = './snowflake4'

# List of all 18 COPY files
COPY_FILES = [
    # Seeds (10 CSV files) - COMPLETED
    'seeds/raw_benchmarks.csv',
    'seeds/raw_cashflows.csv',
    'seeds/raw_counterparties.csv',
    'seeds/raw_dates.csv',
    'seeds/raw_fund_structures.csv',
    'seeds/raw_instruments.csv',
    'seeds/raw_portfolios.csv',
    'seeds/raw_positions.csv',
    'seeds/raw_trades.csv',
    'seeds/raw_valuations.csv',
    # Documentation (3 YAML files) - COMPLETED
    'models/staging/_staging.yml',
    'models/intermediate/_intermediate.yml',
    'models/marts/_marts.yml',
    # Test definitions (1 file) - COMPLETED
    'tests/assert_pnl_balanced.sql',
    # Schema documentation (2 files) - IN PROGRESS
    'schemas/all_schemas_postgres.md',
    'schemas/generate_schemas.py',
    # Configuration (2 files) - COMPLETED
    '.user.yml',
    'packages.yml',
]

def ensure_directories():
    """Create target directories."""
    dirs = {'seeds', 'models/staging', 'models/intermediate', 'models/marts', 'tests', 'schemas'}
    for d in dirs:
        os.makedirs(os.path.join(TARGET_DIR, d), exist_ok=True)

def copy_remaining_files():
    """Copy any remaining files that need copying."""
    print("\n" + "=" * 80)
    print("COPYING REMAINING FILES")
    print("=" * 80 + "\n")
    
    ensure_directories()
    
    # Files that might still need copying
    schema_files = [
        'schemas/all_schemas_postgres.md',
        'schemas/generate_schemas.py',
    ]
    
    for file_path in schema_files:
        source = os.path.join(SOURCE_DIR, file_path)
        target = os.path.join(TARGET_DIR, file_path)
        
        # Skip if already exists
        if os.path.exists(target):
            print(f"✓ {file_path} (already exists)")
            continue
        
        if not os.path.exists(source):
            print(f"❌ SOURCE MISSING: {file_path}")
            continue
        
        try:
            shutil.copy2(source, target)
            print(f"✓ Copied: {file_path}")
        except Exception as e:
            print(f"❌ ERROR: {file_path} - {e}")

def compute_sha256(file_path):
    """Compute SHA256 checksum."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            sha256.update(chunk)
    return sha256.hexdigest()

def verify_all_files():
    """Verify all 18 COPY files are byte-identical."""
    print("\n" + "=" * 80)
    print("VERIFYING ALL 18 COPY FILES")
    print("=" * 80 + "\n")
    
    results = {}
    mismatches = []
    missing = []
    verified = []
    
    for file_path in COPY_FILES:
        source = os.path.join(SOURCE_DIR, file_path)
        target = os.path.join(TARGET_DIR, file_path)
        
        # Check existence
        if not os.path.exists(source):
            print(f"❌ {file_path} - SOURCE MISSING")
            missing.append(file_path)
            results[file_path] = {'status': 'MISSING_SOURCE', 'match': False}
            continue
        
        if not os.path.exists(target):
            print(f"❌ {file_path} - TARGET MISSING")
            missing.append(file_path)
            results[file_path] = {'status': 'MISSING_TARGET', 'match': False}
            continue
        
        # Compute checksums
        src_hash = compute_sha256(source)
        tgt_hash = compute_sha256(target)
        src_size = os.path.getsize(source)
        tgt_size = os.path.getsize(target)
        
        # Compare
        match = (src_hash == tgt_hash and src_size == tgt_size)
        
        if match:
            print(f"✓ {file_path}")
            verified.append(file_path)
            results[file_path] = {
                'status': 'OK',
                'match': True,
                'size': src_size,
                'sha256': src_hash
            }
        else:
            print(f"❌ {file_path} - MISMATCH")
            mismatches.append(file_path)
            results[file_path] = {
                'status': 'MISMATCH',
                'match': False,
                'source_size': src_size,
                'target_size': tgt_size,
                'source_sha256': src_hash,
                'target_sha256': tgt_hash
            }
    
    return results, verified, mismatches, missing

def generate_final_report(results, verified, mismatches, missing):
    """Generate final verification report."""
    report = []
    report.append("\n" + "=" * 80)
    report.append("FINAL COPY FILES VERIFICATION REPORT")
    report.append("=" * 80)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Summary
    total = len(COPY_FILES)
    ok = len(verified)
    report.append("VERIFICATION SUMMARY")
    report.append("-" * 80)
    report.append(f"Total COPY Files: {total}")
    report.append(f"Files Verified (✓): {ok}/{total}")
    report.append(f"Files with Discrepancies: {len(mismatches)}")
    report.append(f"Files Missing: {len(missing)}")
    report.append("")
    
    # Results by category
    report.append("RESULTS BY CATEGORY")
    report.append("-" * 80)
    
    categories = {
        'Seeds (10 CSV)': [f for f in COPY_FILES if f.startswith('seeds/')],
        'Documentation (3 YAML)': [f for f in COPY_FILES if 'models/' in f and f.endswith('.yml')],
        'Test Definitions (1 SQL)': [f for f in COPY_FILES if f.startswith('tests/')],
        'Schema Files (2)': [f for f in COPY_FILES if f.startswith('schemas/')],
        'Configuration (2 YAML)': [f for f in COPY_FILES if '/' not in f],
    }
    
    for cat, files in categories.items():
        ok_in_cat = sum(1 for f in files if f in verified)
        report.append(f"{cat}: {ok_in_cat}/{len(files)}")
        for f in files:
            sym = '✓' if f in verified else '❌'
            report.append(f"  {sym} {f}")
        report.append("")
    
    # Success status
    report.append("SUCCESS CRITERIA")
    report.append("-" * 80)
    if not mismatches and not missing:
        report.append("✓ ALL 18 FILES ARE BYTE-IDENTICAL")
        report.append("✓ All files copied successfully")
        report.append("✓ All checksums match (SHA256)")
        report.append("✓ Zero discrepancies found")
        success = True
    else:
        report.append("❌ VERIFICATION FAILED")
        if mismatches:
            report.append(f"❌ {len(mismatches)} file(s) with mismatched checksums")
        if missing:
            report.append(f"❌ {len(missing)} file(s) missing")
        success = False
    report.append("")
    
    # Verification method
    report.append("VERIFICATION METHOD")
    report.append("-" * 80)
    report.append("Algorithm: SHA256 (256-bit cryptographic hash)")
    report.append("File Size Comparison: Yes")
    report.append("Byte-by-Byte Verification: Complete")
    report.append("")
    
    # Detailed results
    if mismatches or missing:
        report.append("DISCREPANCIES")
        report.append("-" * 80)
        for file_path in mismatches + missing:
            result = results.get(file_path, {})
            report.append(f"❌ {file_path}")
            report.append(f"   Status: {result.get('status', 'UNKNOWN')}")
        report.append("")
    
    report.append("=" * 80 + "\n")
    
    return "\n".join(report), success

def main():
    print("\n╔" + "=" * 78 + "╗")
    print("║" + "COPY FILES VERIFICATION - FINAL".center(78) + "║")
    print("║" + "Verify all 18 COPY files are byte-identical".center(78) + "║")
    print("╚" + "=" * 78 + "╝")
    
    # Copy remaining files
    copy_remaining_files()
    
    # Verify all files
    results, verified, mismatches, missing = verify_all_files()
    
    # Generate report
    report, success = generate_final_report(results, verified, mismatches, missing)
    print(report)
    
    # Save report
    report_file = os.path.join(TARGET_DIR, 'COPY_VERIFICATION_FINAL_REPORT.txt')
    os.makedirs(os.path.dirname(report_file), exist_ok=True)
    with open(report_file, 'w') as f:
        f.write(report)
    print(f"Report saved to: {report_file}\n")
    
    # Save results as JSON
    json_file = os.path.join(TARGET_DIR, 'copy_verification_results.json')
    with open(json_file, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_files': len(COPY_FILES),
            'verified': len(verified),
            'mismatches': len(mismatches),
            'missing': len(missing),
            'files_by_status': {k: v['status'] for k, v in results.items()},
            'success': success
        }, f, indent=2)
    
    # Return exit code
    return 0 if success else 1

if __name__ == '__main__':
    try:
        exit_code = main()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        exit_code = 1
    
    exit(exit_code)
