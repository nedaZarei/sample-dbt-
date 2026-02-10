@echo off
REM ##############################################################################
REM Snowflake Candidate Report Generator (Windows)
REM Task 22: Generate candidate validation report for Snowflake marts
REM
REM Purpose:
REM   - Connects to Snowflake DBT_DEMO.DEV schema
REM   - Extracts data from all 10 marts models
REM   - Computes row counts and SHA256 hashes
REM   - Generates candidate/report.json for comparison with baseline
REM
REM Prerequisites:
REM   - Task 21 completed (all marts built in Snowflake)
REM   - Python 3.x with required packages installed
REM   - Snowflake credentials set in environment variables
REM
REM Usage:
REM   cd benchmark\
REM   generate_candidate_report.bat
REM ##############################################################################

setlocal enabledelayedexpansion

REM Script configuration
set "SCRIPT_DIR=%~dp0"
set "OUTPUT_FILE=candidate\report.json"
set "DIALECT=snowflake"

REM ##############################################################################
REM Main Script
REM ##############################################################################

echo ================================================================================
echo TASK 22: GENERATE SNOWFLAKE CANDIDATE REPORT
echo ================================================================================
echo.

echo This script will:
echo   1. Validate prerequisites
echo   2. Connect to Snowflake DBT_DEMO.DEV schema
echo   3. Query all 10 marts models
echo   4. Compute row counts and data hashes
echo   5. Generate candidate/report.json
echo.

cd /d "%SCRIPT_DIR%"

REM ##############################################################################
REM Validation
REM ##############################################################################

echo [INFO] Checking Snowflake environment variables...

if not defined SNOWFLAKE_ACCOUNT (
    echo [ERROR] SNOWFLAKE_ACCOUNT environment variable not set
    goto :missing_vars
)

if not defined SNOWFLAKE_USER (
    echo [ERROR] SNOWFLAKE_USER environment variable not set
    goto :missing_vars
)

if not defined SNOWFLAKE_PASSWORD (
    echo [ERROR] SNOWFLAKE_PASSWORD environment variable not set
    goto :missing_vars
)

echo [OK] All environment variables set
echo [INFO] Account: %SNOWFLAKE_ACCOUNT%
echo [INFO] User: %SNOWFLAKE_USER%
echo.

REM Check Python
echo [INFO] Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python not found
    echo Please install Python 3.x and add it to PATH
    exit /b 1
)

for /f "tokens=2" %%i in ('python --version 2^>^&1') do set PYTHON_VERSION=%%i
echo [OK] Python %PYTHON_VERSION% found
echo.

REM Check dependencies
echo [INFO] Checking Python dependencies...
python -c "import snowflake.connector" 2>nul
if errorlevel 1 (
    echo [WARN] snowflake-connector-python not found
    echo [INFO] Attempting to install dependencies...
    
    if exist "%SCRIPT_DIR%requirements.txt" (
        python -m pip install -r "%SCRIPT_DIR%requirements.txt" --quiet
        if errorlevel 1 (
            echo [ERROR] Failed to install dependencies
            exit /b 1
        )
        echo [OK] Dependencies installed
    ) else (
        echo [ERROR] requirements.txt not found
        exit /b 1
    )
) else (
    echo [OK] All dependencies available
)
echo.

REM Check script exists
echo [INFO] Checking for generate-report.py...
if not exist "%SCRIPT_DIR%generate-report.py" (
    echo [ERROR] generate-report.py not found in %SCRIPT_DIR%
    exit /b 1
)
echo [OK] generate-report.py found
echo.

REM Check/create output directory
echo [INFO] Checking output directory...
if not exist "%SCRIPT_DIR%candidate" (
    echo [WARN] candidate\ directory not found, creating...
    mkdir "%SCRIPT_DIR%candidate"
    echo [OK] Created candidate\ directory
) else (
    echo [OK] candidate\ directory exists
)
echo.

REM ##############################################################################
REM Generate Report
REM ##############################################################################

echo ================================================================================
echo GENERATING SNOWFLAKE CANDIDATE REPORT
echo ================================================================================
echo.

echo [INFO] Target: Snowflake DBT_DEMO.DEV schema
echo [INFO] Models: 10 marts models
echo [INFO] Output: %OUTPUT_FILE%
echo.
echo [INFO] Executing generate-report.py...
echo.

python generate-report.py --dialect %DIALECT% --output %OUTPUT_FILE%

if errorlevel 1 (
    echo.
    echo [ERROR] Report generation failed
    exit /b 1
)

echo.
echo [OK] Report generation completed successfully
echo.

REM ##############################################################################
REM Verify Output
REM ##############################################################################

echo ================================================================================
echo VERIFYING OUTPUT
echo ================================================================================
echo.

set "OUTPUT_PATH=%SCRIPT_DIR%%OUTPUT_FILE%"

if not exist "%OUTPUT_PATH%" (
    echo [ERROR] Output file not found: %OUTPUT_PATH%
    exit /b 1
)

echo [OK] Output file exists: %OUTPUT_PATH%
echo.

REM Get file size
for %%I in ("%OUTPUT_PATH%") do set FILE_SIZE=%%~zI

if %FILE_SIZE% LSS 100 (
    echo [WARN] Output file is very small ^(%FILE_SIZE% bytes^)
) else (
    echo [OK] Output file size: %FILE_SIZE% bytes
)
echo.

REM Validate JSON structure
echo [INFO] Validating JSON structure...

python -c "import json; import sys; data = json.load(open('%OUTPUT_PATH%')); sys.exit(0 if 'models' in data and len(data['models']) == 10 else 1)" 2>nul

if errorlevel 1 (
    echo [ERROR] JSON validation failed
    exit /b 1
)

echo [OK] JSON structure is valid
echo.

REM Display summary
python -c "import json; data = json.load(open('%OUTPUT_PATH%')); print('Report Summary:'); print('  Dialect:', data.get('dialect')); print('  Database:', data.get('database')); print('  Schema:', data.get('schema')); print('  Generated:', data.get('generated_at')); print('  Models:', len(data.get('models', {}))); print(); print('Model Details:'); [print(f'  * {k}: {v.get(\"row_count\")} rows, hash={v.get(\"data_hash\", \"\")[:16]}...') for k, v in data.get('models', {}).items()]"

echo.

REM ##############################################################################
REM Next Steps
REM ##############################################################################

echo ================================================================================
echo NEXT STEPS
echo ================================================================================
echo.

echo The candidate report has been successfully generated!
echo.
echo Next Task (23): Compare baseline vs candidate reports
echo.
echo To compare reports, run:
echo   cd benchmark\
echo   python compare.py ^
echo     --baseline baseline\report.json ^
echo     --candidate candidate\report.json ^
echo     --output comparison_diff.json
echo.
echo Files generated:
echo   - candidate\report.json (Snowflake validation data)
echo.

echo ================================================================================
echo TASK 22 COMPLETE
echo ================================================================================

exit /b 0

:missing_vars
echo.
echo Please set them using:
echo   set SNOWFLAKE_ACCOUNT=your_account
echo   set SNOWFLAKE_USER=your_user
echo   set SNOWFLAKE_PASSWORD=your_password
echo.
echo Expected account: PZXTMSC-WP48482
echo.
exit /b 1
