#!/usr/bin/env python3
"""Quick test to understand diffs between postgres/ and snowflake/"""

import difflib
import os

def show_diff(filepath):
    """Show diff for a file"""
    pg_path = f'postgres/{filepath}'
    sf_path = f'snowflake/{filepath}'
    
    with open(pg_path, 'r') as f:
        pg_lines = f.readlines()
    
    with open(sf_path, 'r') as f:
        sf_lines = f.readlines()
    
    diff = list(difflib.unified_diff(pg_lines, sf_lines, lineterm=''))
    
    print(f"\n{'='*80}")
    print(f"FILE: {filepath}")
    print(f"{'='*80}")
    print(f"Total diff lines: {len(diff)}")
    
    for i, line in enumerate(diff[:50]):  # Show first 50 lines
        print(line)
    
    if len(diff) > 50:
        print(f"... ({len(diff) - 50} more lines)")

# Test a few files
test_files = [
    'macros/date_utils.sql',
    'models/staging/stg_instruments.sql',
    'models/intermediate/int_irr_calculations.sql',
]

for f in test_files:
    try:
        show_diff(f)
    except Exception as e:
        print(f"Error: {e}")
