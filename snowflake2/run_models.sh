#!/bin/bash

################################################################################
# DBT MODEL EXECUTION SCRIPT
# Purpose: Execute all dbt models in dependency order to build marts
# Task: #21 - Run Snowflake models to build marts
################################################################################

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔═══════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║         DBT Model Execution Script                   ║${NC}"
echo -e "${BLUE}║         Task 21: Run Snowflake Models                ║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════╝${NC}"
echo

# Step 1: Check environment variables
echo -e "${YELLOW}[1/6] Checking environment variables...${NC}"
required_vars=("SNOWFLAKE_ACCOUNT" "SNOWFLAKE_USER" "SNOWFLAKE_PASSWORD")
missing_vars=()

for var in "${required_vars[@]}"; do
    if [ -z "${!var}" ]; then
        missing_vars+=("$var")
    fi
done

if [ ${#missing_vars[@]} -ne 0 ]; then
    echo -e "${RED}✗ Missing required environment variables:${NC}"
    printf '%s\n' "${missing_vars[@]}"
    echo
    echo "Please set them before running:"
    echo "  export SNOWFLAKE_ACCOUNT='your_account'"
    echo "  export SNOWFLAKE_USER='your_user'"
    echo "  export SNOWFLAKE_PASSWORD='your_password'"
    exit 1
fi
echo -e "${GREEN}✓ All environment variables set${NC}"
echo

# Step 2: Check dbt installation
echo -e "${YELLOW}[2/6] Checking dbt installation...${NC}"
if ! command -v dbt &> /dev/null; then
    echo -e "${RED}✗ dbt is not installed${NC}"
    echo "Install with: pip install dbt-snowflake"
    exit 1
fi
echo -e "${GREEN}✓ dbt is installed: $(dbt --version | head -1)${NC}"
echo

# Step 3: Verify we're in the correct directory
echo -e "${YELLOW}[3/6] Verifying directory structure...${NC}"
if [ ! -f "dbt_project.yml" ]; then
    echo -e "${RED}✗ dbt_project.yml not found${NC}"
    echo "Please run this script from the snowflake2/ directory"
    exit 1
fi
if [ ! -f "profiles.yml" ]; then
    echo -e "${RED}✗ profiles.yml not found${NC}"
    exit 1
fi
if [ ! -d "models/staging" ] || [ ! -d "models/intermediate" ] || [ ! -d "models/marts" ]; then
    echo -e "${RED}✗ Required model directories not found${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Directory structure verified${NC}"
echo

# Step 4: Count model files
echo -e "${YELLOW}[4/6] Counting model files...${NC}"
staging_count=$(find models/staging -name "*.sql" | wc -l)
intermediate_count=$(find models/intermediate -name "*.sql" | wc -l)
marts_count=$(find models/marts -name "*.sql" | wc -l)
total_count=$((staging_count + intermediate_count + marts_count))

echo "  📦 Staging models: $staging_count"
echo "  🔧 Intermediate models: $intermediate_count"
echo "  📊 Marts models: $marts_count"
echo "  ━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "  ${GREEN}Total: $total_count models${NC}"
echo

# Step 5: Run dbt models
echo -e "${YELLOW}[5/6] Executing dbt run...${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
echo

# Execute dbt run with profiles-dir flag
if dbt run --profiles-dir . ; then
    echo
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}✓ dbt run completed successfully!${NC}"
    echo
else
    echo
    echo -e "${BLUE}════════════════════════════════════════════════════════${NC}"
    echo -e "${RED}✗ dbt run failed${NC}"
    echo
    echo "To re-run failed models and their dependencies:"
    echo "  dbt run --select state:modified+ --state target/"
    echo
    echo "To run a specific model:"
    echo "  dbt run --select model_name --profiles-dir ."
    exit 1
fi

# Step 6: Summary
echo -e "${YELLOW}[6/6] Execution Summary${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${GREEN}✓ All models executed successfully${NC}"
echo "  📍 Target: DBT_DEMO.DEV schema"
echo "  📊 Models created: $total_count views"
echo "  🔍 Layers: Staging → Intermediate → Marts"
echo

echo -e "${BLUE}Next Steps:${NC}"
echo "  1. Verify views in Snowflake: ./verify_models.sql"
echo "  2. Run dbt tests: dbt test --profiles-dir ."
echo "  3. Generate documentation: dbt docs generate --profiles-dir ."
echo

echo -e "${GREEN}╔═══════════════════════════════════════════════════════╗${NC}"
echo -e "${GREEN}║              🎉 TASK 21 COMPLETE 🎉                   ║${NC}"
echo -e "${GREEN}╚═══════════════════════════════════════════════════════╝${NC}"
