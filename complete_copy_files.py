#!/usr/bin/env python3
"""
Complete the COPY file task by copying all remaining files from postgres/ to snowflake4/
and verifying byte-for-byte identity using SHA256 checksums.

Usage:
    python3 complete_copy_files.py
"""

import os
import shutil
import hashlib
from datetime import datetime
from pathlib import Path

SOURCE_DIR = './postgres'
TARGET_DIR = './snowflake4'

# List of all 18 COPY files
COPY_FILES = [
    # Seeds (10 CSV files)
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
    # Documentation (3 YAML files)
    'models/staging/_staging.yml',
    'models/intermediate/_intermediate.yml',
    'models/marts/_marts.yml',
    # Test definitions (1 file)
    'tests/assert_pnl_balanced.sql',
    # Schema documentation (2 files)
    'schemas/all_schemas_postgres.md',
    'schemas/generate_schemas.py',
    # Configuration (2 files)
    '.user.yml',
    'packages.yml',
]

def ensure_directory_structure():
    """Create necessary directories in target."""
    dirs = {
        'seeds',
        'models/staging',
        'models/intermediate',
        'models/marts',
        'tests',
        'schemas',
    }
    for d in dirs:
        path = os.path.join(TARGET_DIR, d)
        os.makedirs(path, exist_ok=True)
        print(f"✓ Directory: {path}")

def copy_all_files():
    """Copy all COPY files from source to target."""
    print("\n" + "=" * 80)
    print("COPYING ALL 18 COPY FILES")
    print("=" * 80 + "\n")
    
    ensure_directory_structure()
    print()
    
    copied = 0
    failed = []
    
    for file_path in COPY_FILES:
        source = os.path.join(SOURCE_DIR, file_path)
        target = os.path.join(TARGET_DIR, file_path)
        
        if not os.path.exists(source):
            print(f"❌ MISSING SOURCE: {file_path}")
            failed.append(file_path)
            continue
        
        try:
            shutil.copy2(source, target)
            print(f"✓ {file_path}")
            copied += 1
        except Exception as e:
            print(f"❌ ERROR: {file_path} - {e}")
            failed.append(file_path)
    
    print(f"\nCopied: {copied}/{len(COPY_FILES)} files")
    return failed

def compute_sha256(file_path):
    """Compute SHA256 checksum of a file."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            sha256.update(chunk)
    return sha256.hexdigest()

def get_file_size(file_path):
    """Get file size in bytes."""
    return os.path.getsize(file_path)

def verify_all_files():
    """Verify all files are byte-identical."""
    print("\n" + "=" * 80)
    print("VERIFYING CHECKSUMS")
    print("=" * 80 + "\n")
    
    results = []
    mismatches = []
    
    for file_path in COPY_FILES:
        source = os.path.join(SOURCE_DIR, file_path)
        target = os.path.join(TARGET_DIR, file_path)
        
        # Check existence
        if not os.path.exists(source):
            results.append((file_path, 'MISSING_SOURCE', None, None, None, None))
            mismatches.append(file_path)
            print(f"❌ {file_path} - MISSING SOURCE")
            continue
        
        if not os.path.exists(target):
            results.append((file_path, 'MISSING_TARGET', None, None, None, None))
            mismatches.append(file_path)
            print(f"❌ {file_path} - MISSING TARGET")
            continue
        
        # Get sizes
        src_size = get_file_size(source)
        tgt_size = get_file_size(target)
        
        # Compute checksums
        src_hash = compute_sha256(source)
        tgt_hash = compute_sha256(target)
        
        # Compare
        if src_hash == tgt_hash and src_size == tgt_size:
            results.append((file_path, 'OK', src_size, tgt_size, src_hash, tgt_hash))
            print(f"✓ {file_path}")
        else:
            results.append((file_path, 'MISMATCH', src_size, tgt_size, src_hash, tgt_hash))
            mismatches.append(file_path)
            print(f"❌ {file_path}")
            print(f"   Size: {src_size} -> {tgt_size}")
            print(f"   Hash: {src_hash[:16]}... -> {tgt_hash[:16]}...")
    
    return results, mismatches

def generate_report(results, mismatches):
    """Generate verification report."""
    report = []
    report.append("=" * 80)
    report.append("COPY FILES VERIFICATION REPORT")
    report.append("=" * 80)
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("")
    
    ok_count = sum(1 for r in results if r[1] == 'OK')
    report.append(f"Results: {ok_count}/{len(COPY_FILES)} files verified")
    report.append(f"Discrepancies: {len(mismatches)}")
    report.append("")
    
    if mismatches:
        report.append("MISMATCHES:")
        for file_path in mismatches:
            report.append(f"  - {file_path}")
        report.append("")
    else:
        report.append("✓ ALL 18 COPY FILES ARE BYTE-IDENTICAL")
        report.append("")
    
    report.append("Verification Method: SHA256 checksums + file size comparison")
    report.append("=" * 80)
    
    return "\n".join(report)

def main():
    print("\n" + "╔" + "=" * 78 + "╗")
    print("║" + " " * 78 + "║")
    print("║" + "  Complete COPY Files Task - Copy and Verify 18 Files".center(78) + "║")
    print("║" + " " * 78 + "║")
    print("╚" + "=" * 78 + "╝\n")
    
    # Copy files
    copy_failed = copy_all_files()
    
    # Verify files
    results, mismatches = verify_all_files()
    
    # Generate report
    report = generate_report(results, mismatches)
    print("\n" + report)
    
    # Save report
    report_path = os.path.join(TARGET_DIR, 'FINAL_VERIFICATION_REPORT.txt')
    with open(report_path, 'w') as f:
        f.write(report)
    print(f"\nReport saved to: {report_path}\n")
    
    # Exit with appropriate code
    if not mismatches and not copy_failed:
        print("✓✓✓ TASK COMPLETED SUCCESSFULLY ✓✓✓")
        return 0
    else:
        print("❌❌❌ TASK FAILED ❌❌❌")
        if mismatches:
            print(f"Mismatched files: {len(mismatches)}")
        if copy_failed:
            print(f"Failed copies: {len(copy_failed)}")
        return 1

if __name__ == '__main__':
    exit_code = main()
    exit(exit_code)
