#!/usr/bin/env python3
"""
Installation and verification script for benchmark validation dependencies.

This script automates the installation of required Python packages and verifies
that they can be imported successfully.
"""

import subprocess
import sys
import os


def print_header(message):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(f"  {message}")
    print("=" * 70 + "\n")


def run_command(command, description):
    """
    Run a shell command and handle errors.
    
    Args:
        command: Command to run (as list)
        description: Human-readable description
        
    Returns:
        True if successful, False otherwise
    """
    print(f"→ {description}")
    try:
        result = subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True
        )
        if result.stdout:
            print(result.stdout)
        print(f"✓ {description} - SUCCESS\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ {description} - FAILED")
        if e.stdout:
            print("STDOUT:", e.stdout)
        if e.stderr:
            print("STDERR:", e.stderr)
        print()
        return False


def verify_import(module_name, import_statement):
    """
    Verify that a module can be imported.
    
    Args:
        module_name: Human-readable module name
        import_statement: Python import statement to execute
        
    Returns:
        True if successful, False otherwise
    """
    print(f"→ Verifying {module_name} import")
    try:
        exec(import_statement)
        print(f"✓ {module_name} import - SUCCESS\n")
        return True
    except ImportError as e:
        print(f"✗ {module_name} import - FAILED: {e}\n")
        return False


def main():
    """Main installation and verification workflow."""
    print_header("Benchmark Validation Dependencies - Installation & Verification")
    
    # Get the directory containing this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    requirements_file = os.path.join(script_dir, "requirements.txt")
    
    # Check that requirements.txt exists
    if not os.path.exists(requirements_file):
        print(f"✗ ERROR: requirements.txt not found at {requirements_file}")
        sys.exit(1)
    
    print(f"Found requirements.txt at: {requirements_file}\n")
    
    # Read and display requirements
    with open(requirements_file, 'r') as f:
        requirements = f.read().strip().split('\n')
    
    print("Required packages:")
    for req in requirements:
        print(f"  - {req}")
    print()
    
    # Step 1: Upgrade pip
    print_header("Step 1: Upgrade pip")
    success = run_command(
        [sys.executable, "-m", "pip", "install", "--upgrade", "pip"],
        "Upgrading pip to latest version"
    )
    
    if not success:
        print("⚠ Warning: pip upgrade failed, continuing with existing version\n")
    
    # Step 2: Install dependencies
    print_header("Step 2: Install Dependencies")
    success = run_command(
        [sys.executable, "-m", "pip", "install", "-r", requirements_file],
        f"Installing packages from {requirements_file}"
    )
    
    if not success:
        print("✗ ERROR: Failed to install dependencies")
        sys.exit(1)
    
    # Step 3: Verify installations
    print_header("Step 3: Verify Installations")
    
    results = {}
    
    # Verify psycopg2
    results['psycopg2'] = verify_import(
        "psycopg2-binary",
        "import psycopg2"
    )
    
    # Verify snowflake-connector-python
    results['snowflake'] = verify_import(
        "snowflake-connector-python",
        "import snowflake.connector"
    )
    
    # Verify colorama (optional)
    results['colorama'] = verify_import(
        "colorama (optional)",
        "import colorama"
    )
    
    # Summary
    print_header("Installation Summary")
    
    all_required_passed = results['psycopg2'] and results['snowflake']
    
    print("Required packages:")
    print(f"  psycopg2-binary: {'✓ PASS' if results['psycopg2'] else '✗ FAIL'}")
    print(f"  snowflake-connector-python: {'✓ PASS' if results['snowflake'] else '✗ FAIL'}")
    
    print("\nOptional packages:")
    print(f"  colorama: {'✓ PASS' if results['colorama'] else '⚠ NOT INSTALLED (graceful fallback)'}")
    
    print()
    
    if all_required_passed:
        print("=" * 70)
        print("  ✓ SUCCESS: All required dependencies installed and verified!")
        print("=" * 70)
        return 0
    else:
        print("=" * 70)
        print("  ✗ FAILURE: Some required dependencies failed to install")
        print("=" * 70)
        return 1


if __name__ == "__main__":
    sys.exit(main())
