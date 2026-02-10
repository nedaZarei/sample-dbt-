@echo off
REM ==============================================================================
REM Task 23: Compare Baseline vs Candidate Outputs
REM
REM This script automates the comparison workflow for validating that Snowflake
REM translations produce identical outputs to PostgreSQL baseline.
REM
REM Usage:
REM   run_task23.bat
REM
REM Prerequisites:
REM   - PostgreSQL running at localhost:5433
REM   - Snowflake account accessible
REM   - Environment variables set (see below)
REM   - Python dependencies installed (pip install -r requirements.txt)
REM ==============================================================================

echo ================================================================================
echo Task 23: Compare Baseline vs Candidate Outputs
echo ================================================================================
echo.

REM Check if environment variables are set
echo Checking environment variables...

set missing_vars=

if "%DBT_PG_USER%"=="" set missing_vars=%missing_vars% DBT_PG_USER
if "%DBT_PG_PASSWORD%"=="" set missing_vars=%missing_vars% DBT_PG_PASSWORD
if "%DBT_PG_DBNAME%"=="" set missing_vars=%missing_vars% DBT_PG_DBNAME
if "%SNOWFLAKE_ACCOUNT%"=="" set missing_vars=%missing_vars% SNOWFLAKE_ACCOUNT
if "%SNOWFLAKE_USER%"=="" set missing_vars=%missing_vars% SNOWFLAKE_USER
if "%SNOWFLAKE_PASSWORD%"=="" set missing_vars=%missing_vars% SNOWFLAKE_PASSWORD

if not "%missing_vars%"=="" (
    echo X Missing required environment variables:%missing_vars%
    echo.
    echo Please set these variables and try again:
    echo.
    echo   set DBT_PG_USER=postgres
    echo   set DBT_PG_PASSWORD=your_password
    echo   set DBT_PG_DBNAME=bain_analytics
    echo   set SNOWFLAKE_ACCOUNT=your_account
    echo   set SNOWFLAKE_USER=your_user
    echo   set SNOWFLAKE_PASSWORD=your_password
    echo.
    exit /b 1
)

echo √ All required environment variables are set
echo.

REM Run the Python comparison script
echo Running comparison workflow...
echo.

python run_comparison_task23.py

if %ERRORLEVEL% EQU 0 (
    echo.
    echo √ Task 23 completed successfully
) else (
    echo.
    echo X Task 23 failed with exit code %ERRORLEVEL%
)

exit /b %ERRORLEVEL%
