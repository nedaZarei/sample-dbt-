#!/bin/bash
# Installation and verification script for benchmark validation dependencies

set -e  # Exit on error

echo "=============================================================="
echo "  Benchmark Validation Dependencies - Installation"
echo "=============================================================="
echo ""

# Navigate to the script's directory
cd "$(dirname "$0")"

echo "Current directory: $(pwd)"
echo ""

# Check if requirements.txt exists
if [ ! -f "requirements.txt" ]; then
    echo "ERROR: requirements.txt not found!"
    exit 1
fi

echo "Found requirements.txt"
echo ""
echo "Required packages:"
cat requirements.txt | sed 's/^/  - /'
echo ""

# Install dependencies
echo "=============================================================="
echo "  Installing dependencies..."
echo "=============================================================="
echo ""

pip install -r requirements.txt

echo ""
echo "=============================================================="
echo "  Verifying installations..."
echo "=============================================================="
echo ""

# Verify psycopg2
echo "→ Verifying psycopg2..."
python -c "import psycopg2; print('✓ psycopg2-binary: OK')" || {
    echo "✗ psycopg2-binary: FAILED"
    exit 1
}

# Verify snowflake-connector-python
echo "→ Verifying snowflake-connector-python..."
python -c "import snowflake.connector; print('✓ snowflake-connector-python: OK')" || {
    echo "✗ snowflake-connector-python: FAILED"
    exit 1
}

# Verify colorama (optional, don't fail if missing)
echo "→ Verifying colorama (optional)..."
python -c "import colorama; print('✓ colorama: OK')" || {
    echo "⚠ colorama: NOT INSTALLED (graceful fallback available)"
}

echo ""
echo "=============================================================="
echo "  ✓ SUCCESS: All required dependencies installed!"
echo "=============================================================="
echo ""
echo "You can now run the benchmark validation scripts:"
echo "  - python generate-report.py --help"
echo "  - python compare.py --help"
echo ""
