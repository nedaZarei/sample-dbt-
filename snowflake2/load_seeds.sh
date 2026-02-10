#!/bin/bash

################################################################################
# Snowflake Seed Loading Script
# 
# Purpose: Load all 10 seed CSV files into Snowflake DBT_DEMO.RAW schema
# Usage: ./load_seeds.sh
# 
# Prerequisites:
#   - dbt-core and dbt-snowflake installed
#   - Environment variables set: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
#   - DBT_DEMO database and COMPUTE_WH warehouse exist in Snowflake
################################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Script location
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "=========================================="
echo "Snowflake Seed Loading Script"
echo "=========================================="
echo ""

# Check if we're in the right directory
if [ ! -f "$SCRIPT_DIR/profiles.yml" ]; then
    echo -e "${RED}ERROR: profiles.yml not found in current directory${NC}"
    echo "Please run this script from the snowflake2/ directory"
    exit 1
fi

# Check if seeds directory exists
if [ ! -d "$SCRIPT_DIR/seeds" ]; then
    echo -e "${RED}ERROR: seeds/ directory not found${NC}"
    exit 1
fi

# Check environment variables
echo "Checking environment variables..."
if [ -z "$SNOWFLAKE_ACCOUNT" ]; then
    echo -e "${RED}ERROR: SNOWFLAKE_ACCOUNT environment variable not set${NC}"
    echo "Please set it with: export SNOWFLAKE_ACCOUNT='your_account'"
    exit 1
fi

if [ -z "$SNOWFLAKE_USER" ]; then
    echo -e "${RED}ERROR: SNOWFLAKE_USER environment variable not set${NC}"
    echo "Please set it with: export SNOWFLAKE_USER='your_username'"
    exit 1
fi

if [ -z "$SNOWFLAKE_PASSWORD" ]; then
    echo -e "${RED}ERROR: SNOWFLAKE_PASSWORD environment variable not set${NC}"
    echo "Please set it with: export SNOWFLAKE_PASSWORD='your_password'"
    exit 1
fi

echo -e "${GREEN}✓${NC} Environment variables configured"
echo ""

# Count seed files
SEED_COUNT=$(find "$SCRIPT_DIR/seeds" -name "*.csv" | wc -l)
echo "Found $SEED_COUNT seed CSV files"
echo ""

# List seed files
echo "Seed files to be loaded:"
find "$SCRIPT_DIR/seeds" -name "*.csv" -exec basename {} \; | sort
echo ""

# Check if dbt is installed
if ! command -v dbt &> /dev/null; then
    echo -e "${RED}ERROR: dbt command not found${NC}"
    echo "Please install dbt with: pip install dbt-core dbt-snowflake"
    exit 1
fi

echo -e "${GREEN}✓${NC} dbt CLI found: $(dbt --version | head -n 1)"
echo ""

# Prompt for confirmation
read -p "Proceed with loading seeds to Snowflake? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Seed loading cancelled"
    exit 0
fi

echo ""
echo "=========================================="
echo "Running: dbt seed --profiles-dir ."
echo "=========================================="
echo ""

# Run dbt seed
cd "$SCRIPT_DIR"
dbt seed --profiles-dir .

# Check exit code
if [ $? -eq 0 ]; then
    echo ""
    echo "=========================================="
    echo -e "${GREEN}✓ Seed loading completed successfully!${NC}"
    echo "=========================================="
    echo ""
    echo "Expected tables created in DBT_DEMO.RAW:"
    echo "  1. RAW_BENCHMARKS (30 rows)"
    echo "  2. RAW_CASHFLOWS (25 rows)"
    echo "  3. RAW_COUNTERPARTIES (10 rows)"
    echo "  4. RAW_DATES (15 rows)"
    echo "  5. RAW_FUND_STRUCTURES (3 rows)"
    echo "  6. RAW_INSTRUMENTS (20 rows)"
    echo "  7. RAW_PORTFOLIOS (5 rows)"
    echo "  8. RAW_POSITIONS (30 rows)"
    echo "  9. RAW_TRADES (30 rows)"
    echo " 10. RAW_VALUATIONS (21 rows)"
    echo ""
    echo "Next steps:"
    echo "  1. Run staging models: dbt run --select staging.*"
    echo "  2. Run tests: dbt test"
    echo ""
else
    echo ""
    echo "=========================================="
    echo -e "${RED}✗ Seed loading failed${NC}"
    echo "=========================================="
    echo ""
    echo "Troubleshooting tips:"
    echo "  1. Check Snowflake credentials are correct"
    echo "  2. Verify DBT_DEMO database exists"
    echo "  3. Verify COMPUTE_WH warehouse exists"
    echo "  4. Check network connectivity to Snowflake"
    echo "  5. Review error messages above"
    echo ""
    exit 1
fi
