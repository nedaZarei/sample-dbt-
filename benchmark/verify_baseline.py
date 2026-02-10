#!/usr/bin/env python3
"""
Baseline report verification script.

This script validates that a baseline/report.json file meets all success criteria:
- Valid JSON structure
- Contains exactly 10 marts model entries
- All entries have required fields (model_name, row_count, data_hash)
- All row_count values are > 0
- All data_hash values are valid 64-character SHA256 hex strings
"""

import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple


# Expected 10 marts models
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


def print_header(text: str) -> None:
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(text)
    print("=" * 80 + "\n")


def print_section(text: str) -> None:
    """Print a formatted section header."""
    print(f"\n{text}")
    print("-" * 80)


def print_success(text: str) -> None:
    """Print a success message."""
    print(f"✓ {text}")


def print_error(text: str) -> None:
    """Print an error message."""
    print(f"✗ {text}")


def print_warning(text: str) -> None:
    """Print a warning message."""
    print(f"⚠ {text}")


def check_file_exists(file_path: str) -> Tuple[bool, Optional[str]]:
    """
    Check if the baseline report file exists.
    
    Args:
        file_path: Path to the report file
        
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}"
    
    if not os.path.isfile(file_path):
        return False, f"Path is not a file: {file_path}"
    
    file_size = os.path.getsize(file_path)
    if file_size == 0:
        return False, "File is empty"
    
    return True, f"File size: {file_size} bytes"


def load_json(file_path: str) -> Tuple[bool, Any]:
    """
    Load and parse JSON file.
    
    Args:
        file_path: Path to the JSON file
        
    Returns:
        Tuple of (success: bool, data: Any)
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return True, data
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON: {e}"
    except Exception as e:
        return False, f"Error reading file: {e}"


def validate_report_structure(report: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate the top-level structure of the report.
    
    Args:
        report: The parsed JSON report
        
    Returns:
        Tuple of (success: bool, issues: List[str])
    """
    issues = []
    
    # Check required top-level fields
    required_fields = ['generated_at', 'dialect', 'database', 'schema', 'models']
    for field in required_fields:
        if field not in report:
            issues.append(f"Missing required field: {field}")
    
    # Validate dialect
    if 'dialect' in report and report['dialect'] != 'postgres':
        issues.append(f"Expected dialect 'postgres', got '{report['dialect']}'")
    
    # Validate models is a dict
    if 'models' in report and not isinstance(report['models'], dict):
        issues.append(f"Field 'models' should be a dictionary, got {type(report['models']).__name__}")
    
    return len(issues) == 0, issues


def validate_model_count(models: Dict[str, Any]) -> Tuple[bool, List[str]]:
    """
    Validate that exactly 10 models are present.
    
    Args:
        models: The models dictionary from the report
        
    Returns:
        Tuple of (success: bool, issues: List[str])
    """
    issues = []
    
    actual_count = len(models)
    expected_count = len(EXPECTED_MODELS)
    
    if actual_count != expected_count:
        issues.append(f"Expected {expected_count} models, found {actual_count}")
    
    # Check for missing models
    missing_models = set(EXPECTED_MODELS) - set(models.keys())
    if missing_models:
        issues.append(f"Missing models: {', '.join(sorted(missing_models))}")
    
    # Check for extra models
    extra_models = set(models.keys()) - set(EXPECTED_MODELS)
    if extra_models:
        issues.append(f"Unexpected models: {', '.join(sorted(extra_models))}")
    
    return len(issues) == 0, issues


def validate_sha256_hash(hash_value: Any) -> bool:
    """
    Validate that a value is a valid SHA256 hash (64-character hex string).
    
    Args:
        hash_value: The hash value to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not isinstance(hash_value, str):
        return False
    
    if len(hash_value) != 64:
        return False
    
    # Check if it's a valid hex string
    return bool(re.match(r'^[a-fA-F0-9]{64}$', hash_value))


def validate_model_entry(model_name: str, model_data: Any) -> Tuple[bool, List[str]]:
    """
    Validate a single model entry.
    
    Args:
        model_name: Name of the model
        model_data: The model data dictionary
        
    Returns:
        Tuple of (success: bool, issues: List[str])
    """
    issues = []
    
    # Check if model_data is a dict
    if not isinstance(model_data, dict):
        issues.append(f"Model data should be a dictionary, got {type(model_data).__name__}")
        return False, issues
    
    # Check for error field (indicates processing failure)
    if 'error' in model_data:
        issues.append(f"Model has error: {model_data['error']}")
        return False, issues
    
    # Check required fields
    if 'row_count' not in model_data:
        issues.append("Missing field: row_count")
    elif not isinstance(model_data['row_count'], int):
        issues.append(f"row_count should be an integer, got {type(model_data['row_count']).__name__}")
    elif model_data['row_count'] <= 0:
        issues.append(f"row_count is {model_data['row_count']}, expected > 0 (empty model)")
    
    if 'data_hash' not in model_data:
        issues.append("Missing field: data_hash")
    elif not validate_sha256_hash(model_data['data_hash']):
        issues.append(f"Invalid SHA256 hash: {model_data.get('data_hash', 'N/A')}")
    
    return len(issues) == 0, issues


def validate_all_models(models: Dict[str, Any]) -> Tuple[bool, Dict[str, List[str]]]:
    """
    Validate all model entries.
    
    Args:
        models: The models dictionary from the report
        
    Returns:
        Tuple of (success: bool, issues_by_model: Dict[str, List[str]])
    """
    all_valid = True
    issues_by_model = {}
    
    for model_name in EXPECTED_MODELS:
        if model_name not in models:
            continue
        
        valid, issues = validate_model_entry(model_name, models[model_name])
        if not valid:
            all_valid = False
            issues_by_model[model_name] = issues
    
    return all_valid, issues_by_model


def print_model_summary(models: Dict[str, Any]) -> None:
    """
    Print a summary of all models in the report.
    
    Args:
        models: The models dictionary from the report
    """
    print_section("Model Summary")
    
    for model_name in EXPECTED_MODELS:
        if model_name not in models:
            print_error(f"{model_name}: NOT FOUND")
            continue
        
        model_data = models[model_name]
        
        if 'error' in model_data:
            print_error(f"{model_name}: ERROR - {model_data['error']}")
        else:
            row_count = model_data.get('row_count', 'N/A')
            data_hash = model_data.get('data_hash', 'N/A')
            hash_preview = data_hash[:16] + "..." if len(str(data_hash)) > 16 else data_hash
            print_success(f"{model_name}: {row_count} rows, hash={hash_preview}")


def main() -> int:
    """
    Main entry point for the verification script.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print_header("Baseline Report Verification")
    
    report_path = './baseline/report.json'
    
    # Step 1: Check file exists
    print_section("Checking file existence")
    success, message = check_file_exists(report_path)
    if success:
        print_success(f"File found: {report_path}")
        print(f"  {message}")
    else:
        print_error(message)
        print("\nRun 'python capture_baseline.py' to generate the baseline report.")
        return 1
    
    # Step 2: Load JSON
    print_section("Loading JSON")
    success, result = load_json(report_path)
    if not success:
        print_error(result)
        return 1
    
    report = result
    print_success("JSON loaded successfully")
    
    # Step 3: Validate report structure
    print_section("Validating report structure")
    success, issues = validate_report_structure(report)
    if success:
        print_success("Report structure is valid")
        print(f"  Generated at: {report.get('generated_at', 'N/A')}")
        print(f"  Dialect: {report.get('dialect', 'N/A')}")
        print(f"  Database: {report.get('database', 'N/A')}")
        print(f"  Schema: {report.get('schema', 'N/A')}")
    else:
        print_error("Report structure validation failed:")
        for issue in issues:
            print(f"  - {issue}")
        return 1
    
    # Step 4: Validate model count
    models = report.get('models', {})
    print_section("Validating model count")
    success, issues = validate_model_count(models)
    if success:
        print_success(f"Correct number of models: {len(models)}")
    else:
        print_error("Model count validation failed:")
        for issue in issues:
            print(f"  - {issue}")
        return 1
    
    # Step 5: Validate each model entry
    print_section("Validating model entries")
    success, issues_by_model = validate_all_models(models)
    if success:
        print_success("All model entries are valid")
    else:
        print_error(f"Found issues in {len(issues_by_model)} model(s):")
        for model_name, issues in issues_by_model.items():
            print(f"\n  {model_name}:")
            for issue in issues:
                print(f"    - {issue}")
        return 1
    
    # Step 6: Print model summary
    print_model_summary(models)
    
    # Final success message
    print_header("Validation Complete!")
    print("✓ baseline/report.json passes all validation checks")
    print("\nSuccess criteria met:")
    print("  ✓ Valid JSON structure")
    print("  ✓ Contains exactly 10 marts model entries")
    print("  ✓ All entries have required fields (model_name, row_count, data_hash)")
    print("  ✓ All row_count values are > 0")
    print("  ✓ All data_hash values are valid 64-character SHA256 hex strings")
    print("\nNext steps:")
    print("  1. Generate candidate report from Snowflake")
    print("  2. Compare baseline and candidate using compare.py")
    print("\n" + "=" * 80 + "\n")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
