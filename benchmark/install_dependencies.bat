@echo off
REM Installation and verification script for benchmark validation dependencies (Windows)

echo ==============================================================
echo   Benchmark Validation Dependencies - Installation
echo ==============================================================
echo.

REM Navigate to the script's directory
cd /d "%~dp0"

echo Current directory: %CD%
echo.

REM Check if requirements.txt exists
if not exist "requirements.txt" (
    echo ERROR: requirements.txt not found!
    exit /b 1
)

echo Found requirements.txt
echo.
echo Required packages:
type requirements.txt
echo.

REM Install dependencies
echo ==============================================================
echo   Installing dependencies...
echo ==============================================================
echo.

pip install -r requirements.txt
if errorlevel 1 (
    echo ERROR: Failed to install dependencies
    exit /b 1
)

echo.
echo ==============================================================
echo   Verifying installations...
echo ==============================================================
echo.

REM Verify psycopg2
echo ^>^> Verifying psycopg2...
python -c "import psycopg2; print('OK: psycopg2-binary')"
if errorlevel 1 (
    echo FAILED: psycopg2-binary
    exit /b 1
)

REM Verify snowflake-connector-python
echo ^>^> Verifying snowflake-connector-python...
python -c "import snowflake.connector; print('OK: snowflake-connector-python')"
if errorlevel 1 (
    echo FAILED: snowflake-connector-python
    exit /b 1
)

REM Verify colorama (optional)
echo ^>^> Verifying colorama (optional)...
python -c "import colorama; print('OK: colorama')" 2>nul
if errorlevel 1 (
    echo WARNING: colorama not installed (graceful fallback available^)
)

echo.
echo ==============================================================
echo   SUCCESS: All required dependencies installed!
echo ==============================================================
echo.
echo You can now run the benchmark validation scripts:
echo   - python generate-report.py --help
echo   - python compare.py --help
echo.
