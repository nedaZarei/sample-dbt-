#!/bin/bash
################################################################################
# Snowflake Candidate Report Generator
# Task 22: Generate candidate validation report for Snowflake marts
#
# Purpose:
#   - Connects to Snowflake DBT_DEMO.DEV schema
#   - Extracts data from all 10 marts models
#   - Computes row counts and SHA256 hashes
#   - Generates candidate/report.json for comparison with baseline
#
# Prerequisites:
#   - Task 21 completed (all marts built in Snowflake)
#   - Python 3.x with required packages installed
#   - Snowflake credentials set in environment variables
#
# Usage:
#   cd benchmark/
#   chmod +x generate_candidate_report.sh
#   ./generate_candidate_report.sh
################################################################################

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_FILE="candidate/report.json"
DIALECT="snowflake"

################################################################################
# Helper Functions
################################################################################

print_header() {
    echo -e "${BLUE}"
    echo "================================================================================"
    echo "$1"
    echo "================================================================================"
    echo -e "${NC}"
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

print_info() {
    echo -e "${BLUE}ℹ $1${NC}"
}

################################################################################
# Validation Functions
################################################################################

check_environment_variables() {
    print_info "Checking Snowflake environment variables..."
    
    local missing_vars=()
    
    if [[ -z "${SNOWFLAKE_ACCOUNT}" ]]; then
        missing_vars+=("SNOWFLAKE_ACCOUNT")
    fi
    
    if [[ -z "${SNOWFLAKE_USER}" ]]; then
        missing_vars+=("SNOWFLAKE_USER")
    fi
    
    if [[ -z "${SNOWFLAKE_PASSWORD}" ]]; then
        missing_vars+=("SNOWFLAKE_PASSWORD")
    fi
    
    if [[ ${#missing_vars[@]} -gt 0 ]]; then
        print_error "Missing required environment variables:"
        for var in "${missing_vars[@]}"; do
            echo "  - $var"
        done
        echo ""
        echo "Please set them using:"
        echo "  export SNOWFLAKE_ACCOUNT='your_account'"
        echo "  export SNOWFLAKE_USER='your_user'"
        echo "  export SNOWFLAKE_PASSWORD='your_password'"
        echo ""
        echo "Expected account: PZXTMSC-WP48482"
        return 1
    fi
    
    print_success "All environment variables set"
    print_info "Account: ${SNOWFLAKE_ACCOUNT}"
    print_info "User: ${SNOWFLAKE_USER}"
    return 0
}

check_python() {
    print_info "Checking Python installation..."
    
    if ! command -v python3 &> /dev/null; then
        print_error "Python 3 not found"
        echo "Please install Python 3.x"
        return 1
    fi
    
    local python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
    print_success "Python ${python_version} found"
    return 0
}

check_dependencies() {
    print_info "Checking Python dependencies..."
    
    # Check if required packages are installed
    if ! python3 -c "import snowflake.connector" 2>/dev/null; then
        print_warning "snowflake-connector-python not found"
        print_info "Attempting to install dependencies..."
        
        if [[ -f "${SCRIPT_DIR}/requirements.txt" ]]; then
            python3 -m pip install -r "${SCRIPT_DIR}/requirements.txt" --quiet
            if [[ $? -eq 0 ]]; then
                print_success "Dependencies installed"
            else
                print_error "Failed to install dependencies"
                return 1
            fi
        else
            print_error "requirements.txt not found"
            return 1
        fi
    else
        print_success "All dependencies available"
    fi
    
    return 0
}

check_script_exists() {
    print_info "Checking for generate-report.py..."
    
    if [[ ! -f "${SCRIPT_DIR}/generate-report.py" ]]; then
        print_error "generate-report.py not found in ${SCRIPT_DIR}"
        return 1
    fi
    
    print_success "generate-report.py found"
    return 0
}

check_output_directory() {
    print_info "Checking output directory..."
    
    local output_dir="${SCRIPT_DIR}/candidate"
    
    if [[ ! -d "${output_dir}" ]]; then
        print_warning "candidate/ directory not found, creating..."
        mkdir -p "${output_dir}"
        print_success "Created candidate/ directory"
    else
        print_success "candidate/ directory exists"
    fi
    
    return 0
}

################################################################################
# Main Execution Functions
################################################################################

run_report_generation() {
    print_header "GENERATING SNOWFLAKE CANDIDATE REPORT"
    
    print_info "Target: Snowflake DBT_DEMO.DEV schema"
    print_info "Models: 10 marts models"
    print_info "Output: ${OUTPUT_FILE}"
    echo ""
    
    # Run the Python script
    print_info "Executing generate-report.py..."
    echo ""
    
    cd "${SCRIPT_DIR}"
    
    python3 generate-report.py \
        --dialect "${DIALECT}" \
        --output "${OUTPUT_FILE}"
    
    local exit_code=$?
    
    echo ""
    
    if [[ ${exit_code} -eq 0 ]]; then
        print_success "Report generation completed successfully"
        return 0
    else
        print_error "Report generation failed with exit code ${exit_code}"
        return ${exit_code}
    fi
}

verify_output() {
    print_header "VERIFYING OUTPUT"
    
    local output_path="${SCRIPT_DIR}/${OUTPUT_FILE}"
    
    # Check if file exists
    if [[ ! -f "${output_path}" ]]; then
        print_error "Output file not found: ${output_path}"
        return 1
    fi
    
    print_success "Output file exists: ${output_path}"
    
    # Check file size
    local file_size=$(stat -f%z "${output_path}" 2>/dev/null || stat -c%s "${output_path}" 2>/dev/null)
    if [[ ${file_size} -lt 100 ]]; then
        print_warning "Output file is very small (${file_size} bytes)"
    else
        print_success "Output file size: ${file_size} bytes"
    fi
    
    # Validate JSON structure
    if command -v python3 &> /dev/null; then
        local validation_result=$(python3 -c "
import json
import sys

try:
    with open('${output_path}', 'r') as f:
        data = json.load(f)
    
    # Check required fields
    required_fields = ['generated_at', 'dialect', 'database', 'schema', 'models']
    missing_fields = [f for f in required_fields if f not in data]
    
    if missing_fields:
        print(f'MISSING_FIELDS:{missing_fields}')
        sys.exit(1)
    
    # Check models
    models = data.get('models', {})
    model_count = len(models)
    
    if model_count != 10:
        print(f'MODEL_COUNT:{model_count}')
        sys.exit(1)
    
    # Check each model has required fields
    for model_name, metrics in models.items():
        if 'row_count' not in metrics or 'data_hash' not in metrics:
            print(f'INVALID_MODEL:{model_name}')
            sys.exit(1)
        
        if metrics['row_count'] is None or metrics['data_hash'] is None:
            print(f'NULL_VALUES:{model_name}')
            sys.exit(1)
    
    print('VALID')
    sys.exit(0)
    
except json.JSONDecodeError as e:
    print(f'INVALID_JSON:{e}')
    sys.exit(1)
except Exception as e:
    print(f'ERROR:{e}')
    sys.exit(1)
" 2>&1)
        
        local validation_code=$?
        
        if [[ ${validation_code} -eq 0 ]] && [[ "${validation_result}" == "VALID" ]]; then
            print_success "JSON structure is valid"
            
            # Extract and display summary
            python3 -c "
import json

with open('${output_path}', 'r') as f:
    data = json.load(f)

print('\n📊 Report Summary:')
print('  Dialect: {}'.format(data.get('dialect')))
print('  Database: {}'.format(data.get('database')))
print('  Schema: {}'.format(data.get('schema')))
print('  Generated: {}'.format(data.get('generated_at')))
print('  Models: {}'.format(len(data.get('models', {}))))
print('\n📋 Model Details:')

for model_name, metrics in data.get('models', {}).items():
    row_count = metrics.get('row_count', 'N/A')
    data_hash = metrics.get('data_hash', 'N/A')
    hash_preview = data_hash[:16] + '...' if len(data_hash) > 16 else data_hash
    print(f'  ✓ {model_name}: {row_count} rows, hash={hash_preview}')
"
            
        else
            print_error "JSON validation failed: ${validation_result}"
            return 1
        fi
    fi
    
    return 0
}

display_next_steps() {
    print_header "NEXT STEPS"
    
    echo "The candidate report has been successfully generated!"
    echo ""
    echo "Next Task (23): Compare baseline vs candidate reports"
    echo ""
    echo "To compare reports, run:"
    echo "  cd benchmark/"
    echo "  python3 compare.py \\"
    echo "    --baseline baseline/report.json \\"
    echo "    --candidate candidate/report.json \\"
    echo "    --output comparison_diff.json"
    echo ""
    echo "Files generated:"
    echo "  - candidate/report.json (Snowflake validation data)"
    echo ""
}

################################################################################
# Main Script
################################################################################

main() {
    print_header "TASK 22: GENERATE SNOWFLAKE CANDIDATE REPORT"
    
    echo "This script will:"
    echo "  1. Validate prerequisites"
    echo "  2. Connect to Snowflake DBT_DEMO.DEV schema"
    echo "  3. Query all 10 marts models"
    echo "  4. Compute row counts and data hashes"
    echo "  5. Generate candidate/report.json"
    echo ""
    
    # Change to script directory
    cd "${SCRIPT_DIR}"
    
    # Run all validation checks
    if ! check_environment_variables; then
        exit 1
    fi
    
    if ! check_python; then
        exit 1
    fi
    
    if ! check_dependencies; then
        exit 1
    fi
    
    if ! check_script_exists; then
        exit 1
    fi
    
    if ! check_output_directory; then
        exit 1
    fi
    
    echo ""
    
    # Run report generation
    if ! run_report_generation; then
        print_error "Failed to generate report"
        exit 1
    fi
    
    echo ""
    
    # Verify output
    if ! verify_output; then
        print_error "Output verification failed"
        exit 1
    fi
    
    echo ""
    
    # Display next steps
    display_next_steps
    
    print_header "🎉 TASK 22 COMPLETE 🎉"
    
    return 0
}

# Execute main function
main "$@"
