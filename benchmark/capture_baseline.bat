@echo off
REM Automated PostgreSQL baseline capture script for Windows
REM This script captures baseline validation data from PostgreSQL database

setlocal enabledelayedexpansion

echo.
echo ================================================================================
echo PostgreSQL Baseline Capture
echo ================================================================================
echo.

REM Step 1: Check environment variables
echo.
echo [Step 1] Checking environment variables
echo --------------------------------------------------------------------------------

set "missing_vars="
if not defined DBT_PG_USER set "missing_vars=!missing_vars! DBT_PG_USER"
if not defined DBT_PG_PASSWORD set "missing_vars=!missing_vars! DBT_PG_PASSWORD"
if not defined DBT_PG_DBNAME set "missing_vars=!missing_vars! DBT_PG_DBNAME"

if defined missing_vars (
    echo [X] Missing required environment variables:!missing_vars!
    echo.
    echo Required environment variables:
    echo   - DBT_PG_USER
    echo   - DBT_PG_PASSWORD
    echo   - DBT_PG_DBNAME
    echo.
    echo Please set these variables and try again:
    echo   set DBT_PG_USER=postgres
    echo   set DBT_PG_PASSWORD=yourpassword
    echo   set DBT_PG_DBNAME=bain_analytics
    exit /b 1
)

echo [OK] All required environment variables are set

REM Step 2: Check dbt project structure
echo.
echo [Step 2] Checking dbt project structure
echo --------------------------------------------------------------------------------

set "marts_dir=..\postgres\models\marts"
if not exist "%marts_dir%" (
    echo [X] dbt marts directory not found: %marts_dir%
    exit /b 1
)

REM Check for all 10 expected models
set "all_models_found=1"
set "models=fact_portfolio_summary report_portfolio_overview fact_portfolio_pnl fact_trade_activity report_daily_pnl fact_fund_performance fact_cashflow_waterfall fact_portfolio_attribution report_ic_dashboard report_lp_quarterly"

for %%m in (%models%) do (
    if not exist "%marts_dir%\%%m.sql" (
        echo [X] Missing model: %%m
        set "all_models_found=0"
    )
)

if !all_models_found!==0 (
    echo.
    echo Note: This checks for model SQL files, not built models.
    echo Make sure you run 'dbt run' in the postgres\ directory before generating reports.
    exit /b 1
)

echo [OK] All 10 required marts models found

REM Step 3: Check/create baseline directory
echo.
echo [Step 3] Checking baseline directory
echo --------------------------------------------------------------------------------

if not exist ".\baseline" (
    mkdir ".\baseline"
    echo [OK] Created baseline directory: .\baseline
) else (
    echo [OK] Baseline directory exists
)

REM Step 4: Generate baseline report
echo.
echo [Step 4] Generating baseline report
echo --------------------------------------------------------------------------------
echo Running: python generate-report.py --dialect postgres --output baseline\report.json
echo.

python generate-report.py --dialect postgres --output baseline\report.json
if errorlevel 1 (
    echo.
    echo [X] Failed to generate baseline report
    echo.
    echo Troubleshooting:
    echo   - Make sure you've run 'dbt run' in postgres\ directory
    echo   - Check that all marts models are materialized in the database
    echo   - Verify database connection settings
    echo   - Ensure PostgreSQL is running ^(e.g., docker-compose up^)
    exit /b 1
)

REM Step 5: Verify output
echo.
echo [Step 5] Verifying baseline output
echo --------------------------------------------------------------------------------

if not exist ".\baseline\report.json" (
    echo [X] Baseline report not found: .\baseline\report.json
    exit /b 1
)

for %%A in (".\baseline\report.json") do set "file_size=%%~zA"
if !file_size!==0 (
    echo [X] Baseline report is empty
    exit /b 1
)

echo [OK] Baseline report created successfully ^(size: !file_size! bytes^)

REM Final success message
echo.
echo ================================================================================
echo Baseline Capture Complete!
echo ================================================================================
echo.
echo [OK] baseline\report.json has been generated
echo.
echo Next steps:
echo   1. Run 'python verify_baseline.py' to validate the report structure
echo   2. Review baseline\report.json to ensure all models have data
echo   3. Use this baseline to compare against Snowflake candidate reports
echo.
echo ================================================================================
echo.

exit /b 0
