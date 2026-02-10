#!/bin/bash
################################################################################
# Task 23: Compare Baseline vs Candidate Outputs
# 
# This script automates the comparison workflow for validating that Snowflake
# translations produce identical outputs to PostgreSQL baseline.
#
# Usage:
#   chmod +x run_task23.sh
#   ./run_task23.sh
#
# Prerequisites:
#   - PostgreSQL running at localhost:5433
#   - Snowflake account accessible
#   - Environment variables set (see below)
#   - Python dependencies installed (pip install -r requirements.txt)
################################################################################

set -e  # Exit on error

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "================================================================================"
echo "Task 23: Compare Baseline vs Candidate Outputs"
echo "================================================================================"
echo ""

# Check if environment variables are set
echo "Checking environment variables..."

missing_vars=()

# PostgreSQL variables
if [ -z "$DBT_PG_USER" ]; then missing_vars+=("DBT_PG_USER"); fi
if [ -z "$DBT_PG_PASSWORD" ]; then missing_vars+=("DBT_PG_PASSWORD"); fi
if [ -z "$DBT_PG_DBNAME" ]; then missing_vars+=("DBT_PG_DBNAME"); fi

# Snowflake variables
if [ -z "$SNOWFLAKE_ACCOUNT" ]; then missing_vars+=("SNOWFLAKE_ACCOUNT"); fi
if [ -z "$SNOWFLAKE_USER" ]; then missing_vars+=("SNOWFLAKE_USER"); fi
if [ -z "$SNOWFLAKE_PASSWORD" ]; then missing_vars+=("SNOWFLAKE_PASSWORD"); fi

if [ ${#missing_vars[@]} -gt 0 ]; then
    echo -e "${RED}✗ Missing required environment variables:${NC}"
    for var in "${missing_vars[@]}"; do
        echo "  - $var"
    done
    echo ""
    echo "Please set these variables and try again:"
    echo ""
    echo "  export DBT_PG_USER=postgres"
    echo "  export DBT_PG_PASSWORD=your_password"
    echo "  export DBT_PG_DBNAME=bain_analytics"
    echo "  export SNOWFLAKE_ACCOUNT=your_account"
    echo "  export SNOWFLAKE_USER=your_user"
    echo "  export SNOWFLAKE_PASSWORD=your_password"
    echo ""
    exit 1
fi

echo -e "${GREEN}✓ All required environment variables are set${NC}"
echo ""

# Run the Python comparison script
echo "Running comparison workflow..."
echo ""

python3 run_comparison_task23.py

exit_code=$?

if [ $exit_code -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✓ Task 23 completed successfully${NC}"
else
    echo ""
    echo -e "${RED}✗ Task 23 failed with exit code $exit_code${NC}"
fi

exit $exit_code
