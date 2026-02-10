#!/bin/bash
# Automated PostgreSQL baseline capture script for Linux/macOS
# This script captures baseline validation data from PostgreSQL database

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_header() {
    echo -e "\n${BLUE}================================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================================================${NC}\n"
}

print_step() {
    echo -e "\n${BLUE}[Step $1] $2${NC}"
    echo "--------------------------------------------------------------------------------"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

# Main script
print_header "PostgreSQL Baseline Capture"

# Step 1: Check environment variables
print_step 1 "Checking environment variables"

missing_vars=()
[[ -z "$DBT_PG_USER" ]] && missing_vars+=("DBT_PG_USER")
[[ -z "$DBT_PG_PASSWORD" ]] && missing_vars+=("DBT_PG_PASSWORD")
[[ -z "$DBT_PG_DBNAME" ]] && missing_vars+=("DBT_PG_DBNAME")

if [ ${#missing_vars[@]} -ne 0 ]; then
    print_error "Missing required environment variables: ${missing_vars[*]}"
    echo ""
    echo "Required environment variables:"
    echo "  - DBT_PG_USER"
    echo "  - DBT_PG_PASSWORD"
    echo "  - DBT_PG_DBNAME"
    echo ""
    echo "Please set these variables and try again:"
    echo "  export DBT_PG_USER=postgres"
    echo "  export DBT_PG_PASSWORD=yourpassword"
    echo "  export DBT_PG_DBNAME=bain_analytics"
    exit 1
fi

print_success "All required environment variables are set"

# Step 2: Check PostgreSQL connection
print_step 2 "Checking PostgreSQL connection"

if command -v psql &> /dev/null; then
    if PGPASSWORD=$DBT_PG_PASSWORD psql -h localhost -p 5433 -U $DBT_PG_USER -d $DBT_PG_DBNAME -c "SELECT 1" &> /dev/null; then
        print_success "PostgreSQL connection successful"
    else
        print_error "Cannot connect to PostgreSQL at localhost:5433/$DBT_PG_DBNAME"
        echo ""
        echo "Troubleshooting:"
        echo "  - Ensure PostgreSQL is running (e.g., docker-compose up)"
        echo "  - Verify connection details: localhost:5433"
        echo "  - Check database credentials"
        exit 1
    fi
else
    print_warning "psql not found, skipping connection check"
fi

# Step 3: Check dbt project structure
print_step 3 "Checking dbt project structure"

marts_dir="../postgres/models/marts"
if [ ! -d "$marts_dir" ]; then
    print_error "dbt marts directory not found: $marts_dir"
    exit 1
fi

expected_models=(
    "fact_portfolio_summary"
    "report_portfolio_overview"
    "fact_portfolio_pnl"
    "fact_trade_activity"
    "report_daily_pnl"
    "fact_fund_performance"
    "fact_cashflow_waterfall"
    "fact_portfolio_attribution"
    "report_ic_dashboard"
    "report_lp_quarterly"
)

missing_models=()
for model in "${expected_models[@]}"; do
    if [ ! -f "$marts_dir/${model}.sql" ]; then
        missing_models+=("$model")
    fi
done

if [ ${#missing_models[@]} -ne 0 ]; then
    print_error "Missing marts models: ${missing_models[*]}"
    echo ""
    echo "Note: This checks for model SQL files, not built models."
    echo "Make sure you run 'dbt run' in the postgres/ directory before generating reports."
    exit 1
fi

print_success "All 10 required marts models found"

# Step 4: Check/create baseline directory
print_step 4 "Checking baseline directory"

if [ ! -d "./baseline" ]; then
    mkdir -p ./baseline
    print_success "Created baseline directory: ./baseline"
else
    print_success "Baseline directory exists"
fi

# Step 5: Generate baseline report
print_step 5 "Generating baseline report"
echo "Running: python3 generate-report.py --dialect postgres --output baseline/report.json"
echo ""

if python3 generate-report.py --dialect postgres --output baseline/report.json; then
    print_success "Baseline report generated successfully"
else
    print_error "Failed to generate baseline report"
    echo ""
    echo "Troubleshooting:"
    echo "  - Make sure you've run 'dbt run' in postgres/ directory"
    echo "  - Check that all marts models are materialized in the database"
    echo "  - Verify database connection settings"
    exit 1
fi

# Step 6: Verify output
print_step 6 "Verifying baseline output"

if [ ! -f "./baseline/report.json" ]; then
    print_error "Baseline report not found: ./baseline/report.json"
    exit 1
fi

file_size=$(stat -f%z "./baseline/report.json" 2>/dev/null || stat -c%s "./baseline/report.json" 2>/dev/null)
if [ "$file_size" -eq 0 ]; then
    print_error "Baseline report is empty"
    exit 1
fi

print_success "Baseline report created successfully (size: $file_size bytes)"

# Final success message
print_header "Baseline Capture Complete!"
echo -e "${GREEN}✓ baseline/report.json has been generated${NC}"
echo ""
echo "Next steps:"
echo "  1. Run 'python3 verify_baseline.py' to validate the report structure"
echo "  2. Review baseline/report.json to ensure all models have data"
echo "  3. Use this baseline to compare against Snowflake candidate reports"
echo ""
echo "================================================================================"
echo ""
