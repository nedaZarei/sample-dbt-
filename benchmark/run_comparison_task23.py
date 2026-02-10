#!/usr/bin/env python3
"""
Task 23: Compare PostgreSQL baseline vs Snowflake candidate outputs.

This script automates the complete comparison workflow:
1. Generates baseline report from PostgreSQL
2. Generates candidate report from Snowflake
3. Runs comparison between the two reports
4. Documents results for all 10 marts models
"""

import os
import sys
import subprocess
import json
from typing import Tuple, Optional

def print_header(text: str) -> None:
    """Print a formatted header."""
    print("\n" + "=" * 80)
    print(text)
    print("=" * 80 + "\n")


def print_step(step_num: int, text: str) -> None:
    """Print a formatted step message."""
    print(f"\n[Step {step_num}] {text}")
    print("-" * 80)


def print_success(text: str) -> None:
    """Print a success message."""
    print(f"✓ {text}")


def print_error(text: str) -> None:
    """Print an error message."""
    print(f"✗ {text}")


def check_environment_variables() -> Tuple[bool, Optional[str]]:
    """
    Check if required environment variables are set for both PostgreSQL and Snowflake.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    pg_vars = ['DBT_PG_USER', 'DBT_PG_PASSWORD', 'DBT_PG_DBNAME']
    sf_vars = ['SNOWFLAKE_ACCOUNT', 'SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD']
    
    missing_vars = []
    
    for var in pg_vars + sf_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        return False, error_msg
    
    return True, None


def generate_baseline_report() -> Tuple[bool, Optional[str]]:
    """
    Generate baseline report from PostgreSQL.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    try:
        cmd = [
            sys.executable,
            'generate-report.py',
            '--dialect', 'postgres',
            '--output', 'baseline/report.json'
        ]
        
        print("Running: " + " ".join(cmd))
        print()
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Print the output
        if result.stdout:
            print(result.stdout)
        
        return True, None
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to generate baseline report:\n{e.stderr}"
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        return False, error_msg


def generate_candidate_report() -> Tuple[bool, Optional[str]]:
    """
    Generate candidate report from Snowflake.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    try:
        cmd = [
            sys.executable,
            'generate-report.py',
            '--dialect', 'snowflake',
            '--output', 'candidate/report.json'
        ]
        
        print("Running: " + " ".join(cmd))
        print()
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Print the output
        if result.stdout:
            print(result.stdout)
        
        return True, None
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to generate candidate report:\n{e.stderr}"
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error: {e}"
        return False, error_msg


def verify_report(report_path: str, report_name: str) -> Tuple[bool, Optional[str]]:
    """
    Verify that a report exists and contains valid data.
    
    Args:
        report_path: Path to the report file
        report_name: Name of the report for error messages
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    if not os.path.exists(report_path):
        return False, f"{report_name} not found: {report_path}"
    
    file_size = os.path.getsize(report_path)
    if file_size == 0:
        return False, f"{report_name} is empty"
    
    try:
        with open(report_path, 'r') as f:
            report_data = json.load(f)
        
        # Check required fields
        required_fields = ['generated_at', 'dialect', 'database', 'schema', 'models']
        missing_fields = [f for f in required_fields if f not in report_data]
        
        if missing_fields:
            return False, f"{report_name} missing required fields: {', '.join(missing_fields)}"
        
        # Check number of models
        num_models = len(report_data.get('models', {}))
        if num_models != 10:
            return False, f"{report_name} contains {num_models} models, expected 10"
        
        return True, f"Valid ({num_models} models, {file_size} bytes)"
        
    except json.JSONDecodeError as e:
        return False, f"{report_name} contains invalid JSON: {e}"
    except Exception as e:
        return False, f"Error reading {report_name}: {e}"


def run_comparison() -> Tuple[bool, Optional[str]]:
    """
    Run the comparison between baseline and candidate reports.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    try:
        cmd = [
            sys.executable,
            'compare.py',
            '--baseline', 'baseline/report.json',
            '--candidate', 'candidate/report.json',
            '--output', 'comparison_diff.json'
        ]
        
        print("Running: " + " ".join(cmd))
        print()
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
        )
        
        # Print the output (comparison results)
        if result.stdout:
            print(result.stdout)
        
        if result.stderr:
            print(result.stderr)
        
        # Note: compare.py returns exit code 1 if there are failures,
        # but this doesn't mean the script failed - just that there are differences
        return True, f"Comparison completed (exit code: {result.returncode})"
        
    except Exception as e:
        error_msg = f"Unexpected error running comparison: {e}"
        return False, error_msg


def generate_summary_report() -> None:
    """Generate a summary report from the comparison results."""
    try:
        if not os.path.exists('comparison_diff.json'):
            print_error("Comparison diff file not found")
            return
        
        with open('comparison_diff.json', 'r') as f:
            diff_data = json.load(f)
        
        print_header("Task 23 Summary Report")
        
        print("Comparison Details:")
        print(f"  Baseline: {diff_data['baseline_metadata']['dialect']} "
              f"({diff_data['baseline_metadata']['database']}.{diff_data['baseline_metadata']['schema']})")
        print(f"  Candidate: {diff_data['candidate_metadata']['dialect']} "
              f"({diff_data['candidate_metadata']['database']}.{diff_data['candidate_metadata']['schema']})")
        print(f"  Comparison timestamp: {diff_data['comparison_timestamp']}")
        
        summary = diff_data['summary']
        print(f"\nResults Summary:")
        print(f"  Total models: {summary['total_models']}")
        print(f"  ✓ Passed: {summary['passed']}")
        print(f"  ✗ Failed: {summary['failed']}")
        print(f"  ⚠ Missing: {summary['missing']}")
        print(f"  ⚠ Extra: {summary['extra']}")
        
        # List all models with their status
        print(f"\nDetailed Results:")
        
        if diff_data['results']['passed']:
            print("\n  PASSED Models:")
            for result in diff_data['results']['passed']:
                print(f"    ✓ {result['model']} ({result['baseline_row_count']} rows)")
        
        if diff_data['results']['failed']:
            print("\n  FAILED Models:")
            for result in diff_data['results']['failed']:
                print(f"    ✗ {result['model']}")
                for issue in result['issues']:
                    print(f"      - {issue}")
        
        if diff_data['results']['missing']:
            print("\n  MISSING Models (in candidate):")
            for result in diff_data['results']['missing']:
                print(f"    ⚠ {result['model']}")
        
        if diff_data['results']['extra']:
            print("\n  EXTRA Models (not in baseline):")
            for result in diff_data['results']['extra']:
                print(f"    ⚠ {result['model']}")
        
        # Overall status
        print("\n" + "=" * 80)
        if summary['failed'] == 0 and summary['missing'] == 0:
            print("✓ SUCCESS: All 10 marts models PASSED validation!")
            print("  Row counts and data hashes match exactly between baseline and candidate.")
        else:
            print("✗ FAILURE: Some models did not pass validation.")
            print("  Review the failures above and proceed to Task #34 for fixes.")
        print("=" * 80 + "\n")
        
    except Exception as e:
        print_error(f"Error generating summary: {e}")


def main() -> int:
    """
    Main entry point for Task 23 comparison workflow.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print_header("Task 23: Compare Baseline vs Candidate Outputs")
    
    # Step 1: Check environment variables
    print_step(1, "Checking environment variables")
    success, message = check_environment_variables()
    if success:
        print_success("All required environment variables are set")
    else:
        print_error(message)
        print("\nRequired environment variables:")
        print("  PostgreSQL: DBT_PG_USER, DBT_PG_PASSWORD, DBT_PG_DBNAME")
        print("  Snowflake: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD")
        return 1
    
    # Step 2: Generate baseline report (PostgreSQL)
    print_step(2, "Generating baseline report from PostgreSQL")
    success, message = generate_baseline_report()
    if not success:
        print_error(message)
        return 1
    print_success("Baseline report generated")
    
    # Step 3: Verify baseline report
    print_step(3, "Verifying baseline report")
    success, message = verify_report('baseline/report.json', 'Baseline report')
    if success:
        print_success(f"Baseline report verified: {message}")
    else:
        print_error(message)
        return 1
    
    # Step 4: Generate candidate report (Snowflake)
    print_step(4, "Generating candidate report from Snowflake")
    success, message = generate_candidate_report()
    if not success:
        print_error(message)
        return 1
    print_success("Candidate report generated")
    
    # Step 5: Verify candidate report
    print_step(5, "Verifying candidate report")
    success, message = verify_report('candidate/report.json', 'Candidate report')
    if success:
        print_success(f"Candidate report verified: {message}")
    else:
        print_error(message)
        return 1
    
    # Step 6: Run comparison
    print_step(6, "Running comparison between baseline and candidate")
    success, message = run_comparison()
    if not success:
        print_error(message)
        return 1
    print_success(message)
    
    # Step 7: Generate summary report
    print_step(7, "Generating summary report")
    generate_summary_report()
    
    print("\nTask 23 execution completed!")
    print("Review the comparison results above.")
    print("\nGenerated files:")
    print("  - baseline/report.json (PostgreSQL ground truth)")
    print("  - candidate/report.json (Snowflake outputs)")
    print("  - comparison_diff.json (detailed comparison results)")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
