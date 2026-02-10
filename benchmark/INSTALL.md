# Benchmark Validation Dependencies - Installation Guide

This guide provides instructions for installing the Python dependencies required for the benchmark validation infrastructure.

## Quick Start

### Automated Installation (Recommended)

Choose the appropriate script for your platform:

#### Linux/macOS
```bash
cd benchmark/
chmod +x install_dependencies.sh
./install_dependencies.sh
```

#### Windows (Command Prompt)
```cmd
cd benchmark\
install_dependencies.bat
```

#### Cross-Platform (Python)
```bash
cd benchmark/
python install_and_verify.py
```

### Manual Installation

If you prefer to install manually:

```bash
cd benchmark/
pip install -r requirements.txt
```

## Required Dependencies

The following packages are required:

1. **psycopg2-binary** - PostgreSQL database connector
2. **snowflake-connector-python** - Snowflake database connector
3. **colorama** - Optional, for colored terminal output (graceful fallback if missing)

## Verification

After installation, verify that the packages were installed successfully:

### Verify psycopg2
```bash
python -c "import psycopg2; print('OK')"
```

### Verify snowflake-connector-python
```bash
python -c "import snowflake.connector; print('OK')"
```

### Verify colorama (optional)
```bash
python -c "import colorama; print('OK')"
```

If colorama is not installed, the scripts will automatically fall back to plain text output without colors.

## Troubleshooting

### ModuleNotFoundError

If you see `ModuleNotFoundError: No module named 'package_name'`, try:

1. Ensure you're using the correct Python environment
2. Re-run the installation: `pip install -r requirements.txt`
3. Check that pip is using the same Python interpreter: `which python` and `which pip`

### PostgreSQL Driver Issues

If you encounter issues with `psycopg2-binary`, ensure you have:
- A compatible Python version (3.7+)
- Network access to download packages

If binary wheels are not available, try:
```bash
pip install psycopg2-binary --no-cache-dir
```

### Snowflake Connector Issues

If the Snowflake connector fails to install:
1. Check your Python version (3.7+ required)
2. Ensure you have network access
3. Try upgrading pip: `pip install --upgrade pip`

### Virtual Environment (Recommended)

To avoid conflicts with system packages, use a virtual environment:

```bash
cd benchmark/
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Success Criteria

Installation is successful when:
- ✓ All imports succeed without `ModuleNotFoundError`
- ✓ `requirements.txt` dependencies are installed
- ✓ Python can import `psycopg2` and `snowflake.connector` modules
- ✓ No installation errors or dependency conflicts

## Next Steps

After successful installation, you can:

1. Configure environment variables (see `README.md`)
2. Run the baseline report generation: `python generate-report.py --help`
3. Run the comparison tool: `python compare.py --help`

For complete usage documentation, see the main [README.md](README.md) file.
