#!/usr/bin/env python3
"""
Automated PostgreSQL baseline capture script.

This script automates the process of capturing baseline validation data from
the PostgreSQL database. It performs prerequisite checks, generates the baseline
report, and validates the output.
"""

import os
import subprocess
import sys
from typing import Optional, Tuple

try:
    import psycopg2
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    print("Warning: psycopg2 not available. Connection checks will be skipped.")


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


def print_warning(text: str) -> None:
    """Print a warning message."""
    print(f"⚠ {text}")


def check_environment_variables() -> Tuple[bool, Optional[str]]:
    """
    Check if required PostgreSQL environment variables are set.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    required_vars = ['DBT_PG_USER', 'DBT_PG_PASSWORD', 'DBT_PG_DBNAME']
    missing_vars = []
    
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        return False, error_msg
    
    return True, None


def check_postgres_connection() -> Tuple[bool, Optional[str]]:
    """
    Check if PostgreSQL database is accessible.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    if not PSYCOPG2_AVAILABLE:
        return True, "Skipping connection check (psycopg2 not available)"
    
    try:
        host = 'localhost'
        port = 5433
        user = os.getenv('DBT_PG_USER')
        password = os.getenv('DBT_PG_PASSWORD')
        dbname = os.getenv('DBT_PG_DBNAME')
        
        conn = psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=dbname,
            connect_timeout=5
        )
        conn.close()
        return True, None
    except psycopg2.OperationalError as e:
        error_msg = f"Cannot connect to PostgreSQL at {host}:{port}/{dbname}: {e}"
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error connecting to PostgreSQL: {e}"
        return False, error_msg


def check_dbt_project() -> Tuple[bool, Optional[str]]:
    """
    Check if dbt project exists and has the required marts models.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    expected_models = [
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
    
    marts_dir = '../postgres/models/marts'
    if not os.path.exists(marts_dir):
        return False, f"dbt marts directory not found: {marts_dir}"
    
    missing_models = []
    for model in expected_models:
        model_file = os.path.join(marts_dir, f"{model}.sql")
        if not os.path.exists(model_file):
            missing_models.append(model)
    
    if missing_models:
        error_msg = f"Missing marts models: {', '.join(missing_models)}"
        return False, error_msg
    
    return True, None


def check_baseline_directory() -> Tuple[bool, Optional[str]]:
    """
    Check if baseline directory exists, create if needed.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    baseline_dir = './baseline'
    
    if not os.path.exists(baseline_dir):
        try:
            os.makedirs(baseline_dir, exist_ok=True)
            return True, f"Created baseline directory: {baseline_dir}"
        except Exception as e:
            return False, f"Failed to create baseline directory: {e}"
    
    return True, None


def generate_baseline_report() -> Tuple[bool, Optional[str]]:
    """
    Run the generate-report.py script to create baseline/report.json.
    
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
        
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )
        
        # Print the output from generate-report.py
        if result.stdout:
            print(result.stdout)
        
        return True, None
        
    except subprocess.CalledProcessError as e:
        error_msg = f"Failed to generate baseline report:\n{e.stderr}"
        return False, error_msg
    except Exception as e:
        error_msg = f"Unexpected error running generate-report.py: {e}"
        return False, error_msg


def verify_baseline_output() -> Tuple[bool, Optional[str]]:
    """
    Verify that baseline/report.json was created and is valid.
    
    Returns:
        Tuple of (success: bool, error_message: Optional[str])
    """
    report_path = './baseline/report.json'
    
    if not os.path.exists(report_path):
        return False, f"Baseline report not found: {report_path}"
    
    file_size = os.path.getsize(report_path)
    if file_size == 0:
        return False, "Baseline report is empty"
    
    return True, f"Generated report size: {file_size} bytes"


def main() -> int:
    """
    Main entry point for the baseline capture script.
    
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    print_header("PostgreSQL Baseline Capture")
    
    # Step 1: Check environment variables
    print_step(1, "Checking environment variables")
    success, message = check_environment_variables()
    if success:
        print_success("All required environment variables are set")
    else:
        print_error(message)
        print("\nRequired environment variables:")
        print("  - DBT_PG_USER")
        print("  - DBT_PG_PASSWORD")
        print("  - DBT_PG_DBNAME")
        print("\nPlease set these variables and try again.")
        return 1
    
    # Step 2: Check PostgreSQL connection
    print_step(2, "Checking PostgreSQL connection")
    success, message = check_postgres_connection()
    if success:
        if message:
            print_warning(message)
        else:
            print_success("PostgreSQL connection successful")
    else:
        print_error(message)
        print("\nTroubleshooting:")
        print("  - Ensure PostgreSQL is running (e.g., docker-compose up)")
        print("  - Verify connection details: localhost:5433")
        print("  - Check database credentials")
        return 1
    
    # Step 3: Check dbt project structure
    print_step(3, "Checking dbt project structure")
    success, message = check_dbt_project()
    if success:
        print_success("All 10 required marts models found")
    else:
        print_error(message)
        print("\nNote: This checks for model SQL files, not built models.")
        print("Make sure you run 'dbt run' in the postgres/ directory before generating reports.")
        return 1
    
    # Step 4: Check/create baseline directory
    print_step(4, "Checking baseline directory")
    success, message = check_baseline_directory()
    if success:
        if message:
            print_success(message)
        else:
            print_success("Baseline directory exists")
    else:
        print_error(message)
        return 1
    
    # Step 5: Generate baseline report
    print_step(5, "Generating baseline report")
    print("Running: python generate-report.py --dialect postgres --output baseline/report.json\n")
    success, message = generate_baseline_report()
    if not success:
        print_error(message)
        print("\nTroubleshooting:")
        print("  - Make sure you've run 'dbt run' in postgres/ directory")
        print("  - Check that all marts models are materialized in the database")
        print("  - Verify database connection settings")
        return 1
    
    # Step 6: Verify output
    print_step(6, "Verifying baseline output")
    success, message = verify_baseline_output()
    if success:
        print_success(f"Baseline report created successfully ({message})")
    else:
        print_error(message)
        return 1
    
    # Final success message
    print_header("Baseline Capture Complete!")
    print("✓ baseline/report.json has been generated")
    print("\nNext steps:")
    print("  1. Run 'python verify_baseline.py' to validate the report structure")
    print("  2. Review baseline/report.json to ensure all models have data")
    print("  3. Use this baseline to compare against Snowflake candidate reports")
    print("\n" + "=" * 80 + "\n")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
