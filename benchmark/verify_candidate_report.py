#!/usr/bin/env python3
"""
Verify Candidate Report Structure and Content

This script validates that the generated candidate/report.json file has:
- Valid JSON structure
- Required metadata fields
- Exactly 10 marts models
- Non-zero row counts
- Valid SHA256 hashes

Usage:
    python3 verify_candidate_report.py
    python3 verify_candidate_report.py --file candidate/report.json
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from typing import Dict, List, Tuple


# Expected marts models
EXPECTED_MODELS = [
    'fact_portfolio_summary',
    'report_portfolio_overview',
    'fact_portfolio_pnl',
    'fact_trade_activity',
    'report_daily_pnl',
    'fact_fund_performance',
    'fact_cashflow_waterfall',
    'fact_portfolio_attribution',
    'report_ic_dashboard',
    'report_lp_quarterly'
]

# SHA256 hash pattern (64 hex characters)
SHA256_PATTERN = re.compile(r'^[a-f0-9]{64}$', re.IGNORECASE)


class Colors:
    """ANSI color codes for terminal output."""
    GREEN = '\033[0;32m'
    RED = '\033[0;31m'
    YELLOW = '\033[1;33m'
    BLUE = '\033[0;34m'
    RESET = '\033[0m'
    BOLD = '\033[1m'


def print_header(text: str) -> None:
    """Print a formatted header."""
    print(f"\n{Colors.BLUE}{'=' * 80}{Colors.RESET}")
    print(f"{Colors.BOLD}{text}{Colors.RESET}")
    print(f"{Colors.BLUE}{'=' * 80}{Colors.RESET}\n")


def print_success(text: str) -> None:
    """Print success message."""
    print(f"{Colors.GREEN}✓ {text}{Colors.RESET}")


def print_error(text: str) -> None:
    """Print error message."""
    print(f"{Colors.RED}✗ {text}{Colors.RESET}")


def print_warning(text: str) -> None:
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.RESET}")


def print_info(text: str) -> None:
    """Print info message."""
    print(f"{Colors.BLUE}ℹ {text}{Colors.RESET}")


def load_report(file_path: str) -> Tuple[bool, Dict]:
    """
    Load and parse JSON report file.
    
    Args:
        file_path: Path to the report.json file
        
    Returns:
        Tuple of (success, data)
    """
    if not os.path.exists(file_path):
        print_error(f"File not found: {file_path}")
        return False, {}
    
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        print_success(f"Loaded report: {file_path}")
        return True, data
    except json.JSONDecodeError as e:
        print_error(f"Invalid JSON: {e}")
        return False, {}
    except Exception as e:
        print_error(f"Error loading file: {e}")
        return False, {}


def validate_metadata(data: Dict) -> List[str]:
    """
    Validate report metadata fields.
    
    Args:
        data: Report data dictionary
        
    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    
    required_fields = ['generated_at', 'dialect', 'database', 'schema', 'models']
    
    for field in required_fields:
        if field not in data:
            errors.append(f"Missing required field: {field}")
        elif data[field] is None:
            errors.append(f"Field is null: {field}")
    
    # Validate dialect
    if 'dialect' in data and data['dialect'] != 'snowflake':
        errors.append(f"Expected dialect 'snowflake', got '{data['dialect']}'")
    
    # Validate database
    if 'database' in data and data['database'] != 'DBT_DEMO':
        errors.append(f"Expected database 'DBT_DEMO', got '{data['database']}'")
    
    # Validate schema
    if 'schema' in data and data['schema'] != 'DEV':
        errors.append(f"Expected schema 'DEV', got '{data['schema']}'")
    
    # Validate timestamp format
    if 'generated_at' in data:
        try:
            datetime.fromisoformat(data['generated_at'].replace('Z', '+00:00'))
        except ValueError:
            errors.append(f"Invalid timestamp format: {data['generated_at']}")
    
    return errors


def validate_models(data: Dict) -> List[str]:
    """
    Validate models section of report.
    
    Args:
        data: Report data dictionary
        
    Returns:
        List of error messages (empty if valid)
    """
    errors = []
    
    if 'models' not in data:
        errors.append("Missing 'models' section")
        return errors
    
    models = data['models']
    
    if not isinstance(models, dict):
        errors.append("'models' must be a dictionary")
        return errors
    
    # Check model count
    model_count = len(models)
    if model_count != 10:
        errors.append(f"Expected 10 models, found {model_count}")
    
    # Check for expected models
    missing_models = [m for m in EXPECTED_MODELS if m not in models]
    if missing_models:
        errors.append(f"Missing models: {', '.join(missing_models)}")
    
    # Check for unexpected models
    unexpected_models = [m for m in models if m not in EXPECTED_MODELS]
    if unexpected_models:
        errors.append(f"Unexpected models: {', '.join(unexpected_models)}")
    
    # Validate each model entry
    for model_name, metrics in models.items():
        if not isinstance(metrics, dict):
            errors.append(f"{model_name}: metrics must be a dictionary")
            continue
        
        # Check for error field
        if 'error' in metrics:
            errors.append(f"{model_name}: reported error - {metrics['error']}")
            continue
        
        # Check required fields
        if 'row_count' not in metrics:
            errors.append(f"{model_name}: missing row_count")
        elif metrics['row_count'] is None:
            errors.append(f"{model_name}: row_count is null")
        elif not isinstance(metrics['row_count'], int):
            errors.append(f"{model_name}: row_count must be an integer")
        elif metrics['row_count'] <= 0:
            errors.append(f"{model_name}: row_count is zero or negative")
        
        if 'data_hash' not in metrics:
            errors.append(f"{model_name}: missing data_hash")
        elif metrics['data_hash'] is None:
            errors.append(f"{model_name}: data_hash is null")
        elif not isinstance(metrics['data_hash'], str):
            errors.append(f"{model_name}: data_hash must be a string")
        elif not SHA256_PATTERN.match(metrics['data_hash']):
            errors.append(f"{model_name}: invalid SHA256 hash format")
    
    return errors


def display_summary(data: Dict) -> None:
    """
    Display a summary of the report contents.
    
    Args:
        data: Report data dictionary
    """
    print_header("REPORT SUMMARY")
    
    print(f"Dialect:    {data.get('dialect', 'N/A')}")
    print(f"Database:   {data.get('database', 'N/A')}")
    print(f"Schema:     {data.get('schema', 'N/A')}")
    print(f"Generated:  {data.get('generated_at', 'N/A')}")
    print(f"Models:     {len(data.get('models', {}))}")
    print()
    
    if 'models' in data and isinstance(data['models'], dict):
        print_header("MODEL DETAILS")
        
        total_rows = 0
        
        for model_name in sorted(data['models'].keys()):
            metrics = data['models'][model_name]
            
            if 'error' in metrics:
                print_error(f"{model_name}: ERROR - {metrics['error']}")
            else:
                row_count = metrics.get('row_count', 0)
                data_hash = metrics.get('data_hash', '')
                hash_preview = data_hash[:16] + '...' if len(data_hash) > 16 else data_hash
                
                print(f"  {model_name}")
                print(f"    Rows: {row_count:,}")
                print(f"    Hash: {hash_preview}")
                
                total_rows += row_count
        
        print()
        print(f"Total rows across all models: {total_rows:,}")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Verify candidate report structure and content'
    )
    parser.add_argument(
        '--file',
        default='candidate/report.json',
        help='Path to report.json file (default: candidate/report.json)'
    )
    
    args = parser.parse_args()
    
    print_header("CANDIDATE REPORT VERIFICATION")
    
    # Load report
    success, data = load_report(args.file)
    if not success:
        return 1
    
    print()
    
    # Validate metadata
    print_info("Validating metadata...")
    metadata_errors = validate_metadata(data)
    
    if metadata_errors:
        for error in metadata_errors:
            print_error(error)
    else:
        print_success("Metadata is valid")
    
    print()
    
    # Validate models
    print_info("Validating models...")
    model_errors = validate_models(data)
    
    if model_errors:
        for error in model_errors:
            print_error(error)
    else:
        print_success("All models are valid")
    
    print()
    
    # Display summary
    display_summary(data)
    
    # Final result
    total_errors = len(metadata_errors) + len(model_errors)
    
    print_header("VERIFICATION RESULT")
    
    if total_errors == 0:
        print_success("All checks passed! Report is valid.")
        print()
        print("✓ File exists and is readable")
        print("✓ JSON structure is valid")
        print("✓ All required metadata fields present")
        print("✓ Dialect is 'snowflake'")
        print("✓ Database is 'DBT_DEMO'")
        print("✓ Schema is 'DEV'")
        print("✓ All 10 marts models present")
        print("✓ All models have non-zero row counts")
        print("✓ All models have valid SHA256 hashes")
        print()
        print_success("Ready for comparison with baseline (Task 23)")
        return 0
    else:
        print_error(f"Verification failed with {total_errors} error(s)")
        print()
        print("Please fix the errors and regenerate the report:")
        print("  python3 generate-report.py --dialect snowflake --output candidate/report.json")
        return 1


if __name__ == '__main__':
    sys.exit(main())
