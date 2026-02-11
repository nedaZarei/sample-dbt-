#!/usr/bin/env python3
"""
Verify that all 18 COPY files are byte-identical between source (postgres/) and target (snowflake4/).

This script:
1. Copies all 18 COPY files from postgres/ to snowflake4/
2. Generates MD5 and SHA256 checksums for each file
3. Compares checksums to verify byte-identical copies
4. Creates a detailed verification report
"""

import os
import hashlib
import shutil
from pathlib import Path
from datetime import datetime

# Define the 18 COPY files
COPY_FILES = {
    # Seeds (10 CSV files)
    'seeds/raw_benchmarks.csv': 'Raw benchmark performance data',
    'seeds/raw_cashflows.csv': 'Raw cashflow data',
    'seeds/raw_counterparties.csv': 'Raw counterparty reference data',
    'seeds/raw_dates.csv': 'Raw date dimension data',
    'seeds/raw_fund_structures.csv': 'Raw fund structure and fee data',
    'seeds/raw_instruments.csv': 'Raw instrument reference data',
    'seeds/raw_portfolios.csv': 'Raw portfolio data',
    'seeds/raw_positions.csv': 'Raw position data',
    'seeds/raw_trades.csv': 'Raw trade data',
    'seeds/raw_valuations.csv': 'Raw valuation data',
    # Documentation (3 YAML files)
    'models/staging/_staging.yml': 'Staging model documentation',
    'models/intermediate/_intermediate.yml': 'Intermediate model documentation',
    'models/marts/_marts.yml': 'Marts model documentation',
    # Test definitions (1 file)
    'tests/assert_pnl_balanced.sql': 'PnL balance assertion test',
    # Schema documentation (2 files)
    'schemas/all_schemas_postgres.md': 'PostgreSQL schema documentation',
    'schemas/generate_schemas.py': 'Schema documentation generator script',
    # Configuration (2 files)
    '.user.yml': 'User configuration',
    'packages.yml': 'dbt packages configuration',
}

SOURCE_DIR = './postgres'
TARGET_DIR = './snowflake4'

def ensure_target_structure():
    """Create necessary directory structure in target."""
    directories = set()
    for file_path in COPY_FILES.keys():
        if '/' in file_path:
            directories.add(os.path.dirname(file_path))
    
    for dir_name in directories:
        target_path = os.path.join(TARGET_DIR, dir_name)
        os.makedirs(target_path, exist_ok=True)

def copy_files():
    """Copy all COPY files from source to target."""
    print("=" * 80)
    print("COPYING FILES FROM SOURCE TO TARGET")
    print("=" * 80)
    print()
    
    ensure_target_structure()
    
    copied_count = 0
    for file_path, description in COPY_FILES.items():
        source_file = os.path.join(SOURCE_DIR, file_path)
        target_file = os.path.join(TARGET_DIR, file_path)
        
        if not os.path.exists(source_file):
            print(f"❌ MISSING SOURCE: {file_path}")
            continue
        
        try:
            shutil.copy2(source_file, target_file)
            copied_count += 1
            print(f"✓ Copied: {file_path}")
        except Exception as e:
            print(f"❌ ERROR copying {file_path}: {e}")
    
    print()
    print(f"Copied {copied_count}/{len(COPY_FILES)} files")
    print()
    return copied_count == len(COPY_FILES)

def compute_checksum(file_path, algorithm='sha256'):
    """Compute checksum for a file."""
    hash_obj = hashlib.new(algorithm)
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b''):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()

def get_file_size(file_path):
    """Get file size in bytes."""
    return os.path.getsize(file_path)

def verify_checksums():
    """Compute and compare checksums for all COPY files."""
    print("=" * 80)
    print("COMPUTING AND COMPARING CHECKSUMS")
    print("=" * 80)
    print()
    
    results = []
    mismatches = []
    
    for file_path, description in COPY_FILES.items():
        source_file = os.path.join(SOURCE_DIR, file_path)
        target_file = os.path.join(TARGET_DIR, file_path)
        
        # Check if files exist
        if not os.path.exists(source_file):
            results.append({
                'file': file_path,
                'status': 'MISSING_SOURCE',
                'source_size': None,
                'target_size': None,
                'source_sha256': None,
                'target_sha256': None,
                'match': False,
            })
            mismatches.append(file_path)
            continue
        
        if not os.path.exists(target_file):
            results.append({
                'file': file_path,
                'status': 'MISSING_TARGET',
                'source_size': get_file_size(source_file),
                'target_size': None,
                'source_sha256': None,
                'target_sha256': None,
                'match': False,
            })
            mismatches.append(file_path)
            continue
        
        # Get file sizes
        source_size = get_file_size(source_file)
        target_size = get_file_size(target_file)
        
        # Compute checksums
        source_sha256 = compute_checksum(source_file, 'sha256')
        target_sha256 = compute_checksum(target_file, 'sha256')
        
        # Compare
        match = (source_sha256 == target_sha256) and (source_size == target_size)
        
        status = 'OK' if match else 'MISMATCH'
        if source_size != target_size:
            status = 'SIZE_MISMATCH'
        
        result = {
            'file': file_path,
            'status': status,
            'source_size': source_size,
            'target_size': target_size,
            'source_sha256': source_sha256,
            'target_sha256': target_sha256,
            'match': match,
        }
        
        results.append(result)
        
        if not match:
            mismatches.append(file_path)
        
        # Print status
        symbol = '✓' if match else '❌'
        print(f"{symbol} {file_path}")
        print(f"   Size: {source_size:,} bytes (source) vs {target_size:,} bytes (target)")
        print(f"   SHA256: {source_sha256[:16]}... (source)")
        print(f"   SHA256: {target_sha256[:16]}... (target)")
        print()
    
    return results, mismatches

def generate_report(results, mismatches, copy_successful):
    """Generate a detailed verification report."""
    report_lines = []
    
    report_lines.append("=" * 80)
    report_lines.append("COPY FILES VERIFICATION REPORT")
    report_lines.append("=" * 80)
    report_lines.append("")
    report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report_lines.append(f"Source Directory: {SOURCE_DIR}/")
    report_lines.append(f"Target Directory: {TARGET_DIR}/")
    report_lines.append("")
    
    # Summary
    ok_count = sum(1 for r in results if r['match'])
    total_count = len(COPY_FILES)
    
    report_lines.append("SUMMARY")
    report_lines.append("-" * 80)
    report_lines.append(f"Total Files to Verify: {total_count}")
    report_lines.append(f"Files Copied Successfully: {sum(1 for r in results if r['status'] != 'MISSING_SOURCE')}/{total_count}")
    report_lines.append(f"Files with Matching Checksums: {ok_count}/{total_count}")
    report_lines.append(f"Discrepancies Found: {len(mismatches)}")
    report_lines.append("")
    
    # Copy Status
    report_lines.append("COPY OPERATION STATUS")
    report_lines.append("-" * 80)
    if copy_successful:
        report_lines.append("✓ All files copied successfully from source to target")
    else:
        report_lines.append("❌ Some files failed to copy")
    report_lines.append("")
    
    # Verification Results
    report_lines.append("DETAILED VERIFICATION RESULTS")
    report_lines.append("-" * 80)
    report_lines.append("")
    
    for result in results:
        file_path = result['file']
        status = result['status']
        match = result['match']
        
        symbol = '✓' if match else '❌'
        report_lines.append(f"{symbol} {file_path}")
        report_lines.append(f"   Status: {status}")
        report_lines.append(f"   Source Size: {result['source_size']:,} bytes" if result['source_size'] else "   Source Size: N/A")
        report_lines.append(f"   Target Size: {result['target_size']:,} bytes" if result['target_size'] else "   Target Size: N/A")
        
        if result['source_sha256']:
            report_lines.append(f"   Source SHA256: {result['source_sha256']}")
        if result['target_sha256']:
            report_lines.append(f"   Target SHA256: {result['target_sha256']}")
        report_lines.append("")
    
    # File Categories
    report_lines.append("FILE CATEGORIES (18 COPY FILES)")
    report_lines.append("-" * 80)
    report_lines.append("")
    
    report_lines.append("Seeds (10 CSV files):")
    seed_files = [r for r in results if 'seeds/' in r['file']]
    report_lines.append(f"  Total: {len(seed_files)} files")
    report_lines.append(f"  Verified: {sum(1 for r in seed_files if r['match'])}/{len(seed_files)}")
    for result in seed_files:
        symbol = '✓' if result['match'] else '❌'
        report_lines.append(f"  {symbol} {result['file']}")
    report_lines.append("")
    
    report_lines.append("Documentation (3 YAML files):")
    doc_files = [r for r in results if 'models/' in r['file'] and r['file'].endswith('.yml')]
    report_lines.append(f"  Total: {len(doc_files)} files")
    report_lines.append(f"  Verified: {sum(1 for r in doc_files if r['match'])}/{len(doc_files)}")
    for result in doc_files:
        symbol = '✓' if result['match'] else '❌'
        report_lines.append(f"  {symbol} {result['file']}")
    report_lines.append("")
    
    report_lines.append("Test definitions (1 SQL file):")
    test_files = [r for r in results if 'tests/' in r['file']]
    report_lines.append(f"  Total: {len(test_files)} files")
    report_lines.append(f"  Verified: {sum(1 for r in test_files if r['match'])}/{len(test_files)}")
    for result in test_files:
        symbol = '✓' if result['match'] else '❌'
        report_lines.append(f"  {symbol} {result['file']}")
    report_lines.append("")
    
    report_lines.append("Schema documentation (2 files):")
    schema_files = [r for r in results if 'schemas/' in r['file']]
    report_lines.append(f"  Total: {len(schema_files)} files")
    report_lines.append(f"  Verified: {sum(1 for r in schema_files if r['match'])}/{len(schema_files)}")
    for result in schema_files:
        symbol = '✓' if result['match'] else '❌'
        report_lines.append(f"  {symbol} {result['file']}")
    report_lines.append("")
    
    report_lines.append("Configuration (2 files):")
    config_files = [r for r in results if '/' not in r['file'] and r['file'].endswith('.yml')]
    report_lines.append(f"  Total: {len(config_files)} files")
    report_lines.append(f"  Verified: {sum(1 for r in config_files if r['match'])}/{len(config_files)}")
    for result in config_files:
        symbol = '✓' if result['match'] else '❌'
        report_lines.append(f"  {symbol} {result['file']}")
    report_lines.append("")
    
    # Discrepancies
    if mismatches:
        report_lines.append("DISCREPANCIES DETECTED")
        report_lines.append("-" * 80)
        for file_path in mismatches:
            result = next(r for r in results if r['file'] == file_path)
            report_lines.append(f"❌ {file_path}")
            report_lines.append(f"   Status: {result['status']}")
            if result['source_sha256'] and result['target_sha256']:
                report_lines.append(f"   Source SHA256: {result['source_sha256']}")
                report_lines.append(f"   Target SHA256: {result['target_sha256']}")
                report_lines.append(f"   Difference: Files are NOT byte-identical")
            report_lines.append("")
    else:
        report_lines.append("SUCCESS CRITERIA")
        report_lines.append("-" * 80)
        report_lines.append("✓ All 18 files have matching checksums")
        report_lines.append("✓ All files are byte-identical between source and target")
        report_lines.append("✓ Zero discrepancies found")
        report_lines.append("")
    
    # Verification Method
    report_lines.append("VERIFICATION METHOD")
    report_lines.append("-" * 80)
    report_lines.append("Algorithm: SHA256 (256-bit cryptographic hash)")
    report_lines.append("File Size Comparison: Yes")
    report_lines.append("Byte-by-Byte Verification: SHA256 hash provides cryptographic assurance")
    report_lines.append("")
    
    report_lines.append("=" * 80)
    
    return "\n".join(report_lines)

def main():
    print()
    print("╔════════════════════════════════════════════════════════════════════════════════╗")
    print("║        COPY FILES BYTE-IDENTICAL VERIFICATION                                 ║")
    print("║        Verifying 18 COPY files between postgres/ and snowflake4/              ║")
    print("╚════════════════════════════════════════════════════════════════════════════════╝")
    print()
    
    # Step 1: Copy files
    copy_successful = copy_files()
    
    # Step 2: Verify checksums
    results, mismatches = verify_checksums()
    
    # Step 3: Generate report
    report = generate_report(results, mismatches, copy_successful)
    
    # Print and save report
    print(report)
    
    # Save report to file
    report_file = os.path.join(TARGET_DIR, 'VERIFICATION_REPORT.txt')
    with open(report_file, 'w') as f:
        f.write(report)
    
    print(f"\nReport saved to: {report_file}")
    
    # Final status
    print()
    if not mismatches and copy_successful:
        print("✓✓✓ VERIFICATION PASSED ✓✓✓")
        print("All 18 COPY files are byte-identical between source and target.")
        return 0
    else:
        print("❌❌❌ VERIFICATION FAILED ❌❌❌")
        if mismatches:
            print(f"Found {len(mismatches)} file(s) with discrepancies:")
            for file_path in mismatches:
                print(f"  - {file_path}")
        return 1

if __name__ == "__main__":
    exit(main())
