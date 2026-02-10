# Benchmarking System

A comprehensive performance benchmarking system for dbt pipelines running on Snowflake. This tool enables data engineers and analytics engineers to measure SQL optimization impact, track performance regressions, and maintain performance baselines across three distinct pipeline configurations.

## Table of Contents

1. [Overview](#overview)
2. [Key Features](#key-features)
3. [Prerequisites](#prerequisites)
4. [Installation](#installation)
5. [Workflow](#workflow)
6. [Commands](#commands)
7. [Metrics Reference](#metrics-reference)
8. [JSON Schema](#json-schema)
9. [Troubleshooting](#troubleshooting)
10. [Examples](#examples)

## Overview

The benchmarking system is an integrated solution for measuring and analyzing the performance of dbt pipelines. It automates the collection of query metrics from Snowflake, verifies output consistency, and compares results against established baselines to detect performance regressions.

## Key Features

- **Automated Metrics Collection**: Extracts execution time, bytes scanned, rows produced, and partitions scanned metrics from Snowflake query history
- **Baseline Management**: Create and store performance baselines as version-controlled JSON files
- **Performance Comparison**: Compare current pipeline results against baselines to identify regressions
- **Output Verification**: Verifies that pipeline outputs remain consistent using content hashing
- **Multi-Pipeline Support**: Benchmark three independent pipelines (A, B, C) or all simultaneously
- **Detailed Reporting**: Generates JSON reports with percentage changes and warnings for regressions
- **Color-Coded Output**: Terminal output with visual indicators for improvements, regressions, and warnings

## Prerequisites

Before using the benchmarking system, ensure you have:

### Required Software

- **Python**: 3.7 or higher
- **dbt-core**: 1.5.0 or higher
- **dbt-snowflake**: 1.5.0 or higher (installed via dbt-core)

### Snowflake Access

- **Active Snowflake Account**: Access to a Snowflake instance with your project data
- **Warehouse Access**: Read access to the warehouse where dbt pipelines execute
- **Query History Access**: Permission to query `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` (requires ACCOUNTADMIN role or equivalent permissions)
- **Database/Schema Access**: Read access to your target database and schemas

### Required Permissions

- Query Snowflake account usage views (for metrics collection)
- Read model outputs and compute row counts/hashes (for verification)
- Write to the benchmark results directory (optional, for local results)

### Configuration Files

- **~/.dbt/profiles.yml**: dbt profile configuration with Snowflake credentials
- **snowflake/benchmark/benchmark_config.yml**: Pipeline definitions (pre-configured)

### Environment Variables (Alternative to profiles.yml)

If not using profiles.yml, set these environment variables:

```bash
export SNOWFLAKE_ACCOUNT=your_account_identifier
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH  # optional
export SNOWFLAKE_DATABASE=DBT_DEMO     # optional
export SNOWFLAKE_SCHEMA=DEV            # optional
```

## Installation

### Step 1: Install Dependencies

Navigate to the benchmark directory and install required Python packages:

```bash
cd snowflake/benchmark
pip install -r requirements.txt
```

The requirements.txt includes:

- `dbt-snowflake>=1.5.0`: Snowflake adapter for dbt
- `dbt-core>=1.5.0`: dbt core framework
- `snowflake-connector-python>=3.1.0`: Snowflake Python connector
- `pyyaml>=6.0`: YAML configuration parsing

### Step 2: Configure Snowflake Connection

Ensure your dbt profiles.yml is properly configured (typically at `~/.dbt/profiles.yml`):

```yaml
bain_capital_analytics:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      warehouse: COMPUTE_WH
      database: DBT_DEMO
      role: ACCOUNTADMIN
      schema: DEV
      threads: 4
```

Or set environment variables directly:

```bash
export SNOWFLAKE_ACCOUNT=xy12345
export SNOWFLAKE_USER=your_email@company.com
export SNOWFLAKE_PASSWORD=your_password
```

### Step 3: Verify Installation

Test your setup by checking help information:

```bash
python benchmark.py --help
```

You should see the available commands: `run`, `save-baseline`, and `compare`.

## Workflow

The benchmarking system follows a four-step workflow:

### 1. Run Pipeline and Collect Metrics

Execute a pipeline and collect performance metrics:

```bash
cd snowflake/benchmark
python benchmark.py run a
```

This command:

- Executes the dbt pipeline using the configured selector
- Collects execution metrics from Snowflake query history
- Verifies output consistency by computing row counts and content hashes
- Saves results to `results/pipeline_a_result.json`

### 2. Save Baseline (First Run)

After establishing a stable baseline, save the metrics as a reference point:

```bash
python benchmark.py save-baseline a
```

This command:

- Loads the latest run results
- Creates a baseline file at `baselines/pipeline_a_baseline.json`
- Includes metrics, output fingerprints, and metadata (git commit, dbt version, user)

### 3. Make Optimizations

Optimize your SQL models, transformations, or data structures. For example:

- Add clustered keys or change clustering strategy
- Optimize joins or aggregations
- Adjust partition pruning
- Improve model materialization strategies

### 4. Run and Compare

Execute the pipeline again and compare against the baseline:

```bash
python benchmark.py run a
python benchmark.py compare a
```

This command:

- Loads current results and the saved baseline
- Calculates percentage changes for each metric
- Identifies regressions (increases in execution time or bytes scanned)
- Displays a comparison report with color-coded results

## Commands

### Command: run

Execute a pipeline and collect performance metrics.

**Syntax:**

```bash
python benchmark.py run <pipeline> [--no-verify] [--verbose]
```

**Arguments:**

- `pipeline`: Pipeline to run (a, b, c, or all)

**Options:**

- `--no-verify`: Skip output verification step (faster, but doesn't check for unexpected data changes)
- `--verbose`: Enable detailed logging output

**Examples:**

```bash
# Run benchmark for Pipeline A
python benchmark.py run a

# Run all three pipelines with verbose output
python benchmark.py run all --verbose

# Run Pipeline B without output verification
python benchmark.py run b --no-verify

# Run all pipelines quietly
python benchmark.py run all
```

**Process:**

1. **Step 1/4**: Execute pipeline using dbt with configured selector
2. **Step 2/4**: Collect metrics from Snowflake query history for executed models
3. **Step 3/4**: Verify model outputs haven't unexpectedly changed
4. **Step 4/4**: Save results to `results/pipeline_X_result.json`

**Exit Codes:**

- `0`: Success
- `1`: Benchmark execution failed
- `2`: Output verification failed
- `3`: Invalid arguments

### Command: save-baseline

Save current run results as a baseline for future comparison.

**Syntax:**

```bash
python benchmark.py save-baseline <pipeline> [--force] [--verbose]
```

**Arguments:**

- `pipeline`: Pipeline to save baseline for (a, b, c, or all)

**Options:**

- `--force`: Overwrite existing baseline without warning
- `--verbose`: Enable detailed logging output

**Examples:**

```bash
# Save baseline for Pipeline A
python benchmark.py save-baseline a

# Overwrite existing baseline for Pipeline C
python benchmark.py save-baseline c --force

# Save baselines for all pipelines
python benchmark.py save-baseline all --force
```

**Process:**

1. Loads the latest run results from `results/pipeline_X_result.json`
2. Extracts metrics and output fingerprints
3. Captures metadata (timestamp, git commit, dbt version, username)
4. Saves to `baselines/pipeline_X_baseline.json`

**Notes:**

- A baseline file must exist before running `compare`
- Use `--force` to update an existing baseline after optimization
- Baseline files are version-controlled and shared across the team

### Command: compare

Compare current run results against a saved baseline.

**Syntax:**

```bash
python benchmark.py compare <pipeline> [--verbose]
```

**Arguments:**

- `pipeline`: Pipeline to compare (a, b, c, or all)

**Options:**

- `--verbose`: Enable detailed logging output

**Examples:**

```bash
# Compare Pipeline A against baseline
python benchmark.py compare a

# Compare all pipelines
python benchmark.py compare all

# Compare with verbose output
python benchmark.py compare a --verbose
```

**Process:**

1. Loads current results from `results/pipeline_X_result.json`
2. Loads baseline from `baselines/pipeline_X_baseline.json`
3. Calculates percentage change for each metric
4. Identifies regressions (negative performance changes)
5. Displays comparison results with color-coded output

**Output Format:**

```
======================================================================
Pipeline: Pipeline A
Timestamp: 2024-01-15T10:30:45.123456+00:00
======================================================================

Metric Comparisons:
Metric                     Baseline        Current         Change
----------------------------------------------------------------------
Execution Time             5000            4750            -5.0% ✓ IMPROVED
Bytes Scanned              1000000         950000          -5.0% ✓ IMPROVED
Rows Produced              10000           10000           0.0% → NEUTRAL

======================================================================
```

## Metrics Reference

The benchmarking system tracks the following metrics at the pipeline level:

### execution_time (or total_execution_time_ms)

**What it measures:** Total time (in milliseconds) to execute all queries in the pipeline

**Why it matters:**

- Primary indicator of pipeline performance
- Impacts data warehouse costs (longer execution = higher compute costs)
- Affects downstream dependencies and SLAs

**How to interpret:**

- Negative change = improvement (faster execution)
- Positive change = regression (slower execution)
- Target: Minimize or maintain execution time during optimization

**Example:** If baseline is 5000ms and current is 5500ms, that's a +10% regression.

### bytes_scanned

**What it measures:** Total data scanned from Snowflake tables (in bytes) during pipeline execution

**Why it matters:**

- Indicates query efficiency
- Affects Snowflake storage scan costs
- Shows effectiveness of partitioning and clustering strategies

**How to interpret:**

- Negative change = improvement (scanning less data)
- Positive change = regression (scanning more data)
- Target: Minimize bytes scanned while maintaining accuracy

**Example:** If baseline scans 1GB and current scans 1.2GB, that's a +20% regression.

### rows_produced

**What it measures:** Total rows created or processed by the pipeline

**Why it matters:**

- Indicates data volume and model accuracy
- Helps detect unintended data expansion or filtering
- Important for capacity planning

**How to interpret:**

- Positive change = warning (more data than baseline, unexpected expansion)
- Negative change = warning (less data than baseline, unexpected filtering)
- Target: Remain constant unless intentional changes were made

**Example:** If baseline produces 10,000 rows and current produces 12,000 rows, investigate if this is intentional.

### partitions_scanned / partitions_total

**What it measures:** Number of table partitions accessed vs. total available partitions

**Why it matters:**

- Shows partition pruning effectiveness
- Low ratio indicates good clustering strategy
- High ratio suggests missing or ineffective clustered keys

**How to interpret:**

- Lower ratio = better partition pruning
- If ratio increases, review clustering strategy
- Target: Keep partition scan ratio as low as possible

**Example:** Scanning 50 partitions out of 1000 total = 5% scan ratio (good).

## JSON Schema

Baseline and report files use JSON format for machine readability and version control compatibility. This section documents the structure of these files.

### Baseline JSON Schema

**File location:** `baselines/pipeline_X_baseline.json`

**Top-level fields:**

- `pipeline_name` (string): Name of the pipeline (e.g., "Pipeline A")
- `metrics` (object): Aggregated performance metrics
- `fingerprints` (array): Output verification hashes for model rows
- `metadata` (object): Execution context information

**metrics object:**

- `pipeline_id` (string): Single-character pipeline ID (A, B, or C)
- `timestamp` (string): ISO 8601 timestamp of baseline creation
- `total_execution_time_ms` (number): Total execution time in milliseconds
- `total_bytes_scanned` (number): Total bytes scanned in the pipeline
- `total_rows_produced` (number): Total rows produced by all models
- `model_metrics` (array): Per-model breakdown of metrics

**model_metrics array items:**

- `model_name` (string): dbt model name
- `fqn` (string): Fully qualified name (database.schema.model)
- `total_execution_time_ms` (number): Model execution time
- `total_bytes_scanned` (number): Bytes scanned by model
- `total_rows_produced` (number): Rows produced by model
- `query_count` (number): Number of queries executed for model

**fingerprints array items:**

- `model_name` (string): dbt model name
- `fqn` (string): Fully qualified name
- `row_count` (number): Number of rows in model output
- `content_hash` (string): SHA256 hash of output data
- `timestamp` (string): ISO 8601 timestamp of fingerprint collection

**metadata object:**

- `timestamp` (string): ISO 8601 timestamp when baseline was saved
- `git_commit` (string, nullable): Git commit hash of the codebase
- `dbt_version` (string, nullable): dbt version used
- `username` (string, nullable): Username who created the baseline

### Report JSON Schema

**File location:** `results/YYYYMMDD_HHMMSS_pipeline_X_report.json`

**Top-level fields:**

- `pipeline` (string): Pipeline name
- `timestamp` (string): ISO 8601 timestamp of comparison
- `baseline_metrics` (object): Metrics from baseline file
- `current_metrics` (object): Metrics from current run
- `comparison` (object): Percentage changes for each metric
- `warnings` (array): List of detected regressions
- `verification_status` (string): "pass" or "fail"

**comparison object:**

- `execution_time_change_pct` (number): Percentage change in execution time
- `bytes_scanned_change_pct` (number): Percentage change in bytes scanned
- `rows_produced_change_pct` (number): Percentage change in rows produced
- `partitions_scanned_change_pct` (number): Percentage change in partitions scanned

**warnings array items (strings):**

- "execution_time increased by X%"
- "bytes_scanned increased by X%"
- "partitions_scanned increased by X%"

## Troubleshooting

### Connection Issues

**Error:** "Could not load Snowflake credentials from environment variables"

**Causes:**

- Missing environment variables (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD)
- Incorrect dbt profile configuration
- Invalid credentials

**Solutions:**

1. Verify environment variables are set:
   ```bash
   echo $SNOWFLAKE_ACCOUNT
   echo $SNOWFLAKE_USER
   ```
2. Check dbt profile at ~/.dbt/profiles.yml exists and is valid
3. Test connection manually:
   ```bash
   dbt debug
   ```
4. Verify account identifier format (should be like xy12345, not xy12345.us-east-1)

### Missing Metrics

**Error:** "Metrics collected for pipeline A, but some values are 0"

**Causes:**

- Pipeline execution was very fast (sub-millisecond queries not captured)
- Queries completed before Snowflake recorded them in query history
- Delay in query history availability

**Solutions:**

1. Wait 5-10 seconds after run completes before collecting metrics
2. Check SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY for your executed queries:
   ```sql
   SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
   WHERE USER_NAME = 'your_user'
   ORDER BY START_TIME DESC
   LIMIT 10;
   ```
3. Verify Snowflake role has ACCOUNT_USAGE permissions
4. Ensure warehouse where dbt runs is accessible

### Verification Failures

**Error:** "Output verification failed. Passed 2/3 models"

**Causes:**

- Model output data changed (row count or content mismatch)
- Unexpected data modifications
- Schema changes to models

**Solutions:**

1. Review what changed in the failing models:
   ```bash
   dbt docs generate && dbt docs serve
   ```
2. Check dbt run logs for SQL errors or unexpected transformations
3. Verify source data hasn't changed
4. If changes are intentional, update the baseline:
   ```bash
   python benchmark.py save-baseline a --force
   ```

### Git Not Available

**Warning:** "Git not installed or not in PATH" or similar message during baseline save

**Causes:**

- Git is not installed on system
- Git is installed but not in system PATH
- Running in environment without git

**Solutions:**

1. Install git if needed:
   ```bash
   # macOS
   brew install git
   
   # Linux
   sudo apt-get install git
   
   # Windows
   # Download from https://git-scm.com
   ```
2. If running in Docker/container without git, the system will gracefully continue without git commit hash
3. This is a non-critical warning; baselines will be saved without the git_commit field if git is unavailable

### No Baseline Found

**Error:** "No baseline found for Pipeline A. Save baseline first."

**Causes:**

- First run of benchmarking system
- Baseline file was deleted
- Using different pipeline than intended

**Solutions:**

1. Create initial baseline:
   ```bash
   python benchmark.py run a
   python benchmark.py save-baseline a
   ```
2. Verify baseline file exists:
   ```bash
   ls -la baselines/pipeline_a_baseline.json
   ```
3. Check that you're comparing the correct pipeline

### Large Performance Changes

**Warning:** "execution_time increased by 45%"

**Interpretation:**

- This is a significant regression that likely needs investigation
- Could indicate query inefficiency, data growth, or system changes

**Solutions:**

1. Check what changed since last baseline:
   ```bash
   git log --oneline -10
   ```
2. Review recent SQL model changes
3. Check if data volume has grown significantly
4. Monitor Snowflake query performance:
   ```sql
   SELECT QUERY_ID, EXECUTION_TIME, BYTES_SCANNED
   FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
   WHERE QUERY_EXECUTION_TIME > 60000  -- queries over 1 minute
   ORDER BY START_TIME DESC;
   ```
5. After investigation, re-optimize and save new baseline if satisfied

## Examples

### Example Workflow

```bash
# Step 1: Run baseline collection
cd snowflake/benchmark
python benchmark.py run a
python benchmark.py save-baseline a

# Step 2: Make optimizations to your dbt models
# (e.g., add clustering, optimize joins, etc.)

# Step 3: Run benchmark again and compare
python benchmark.py run a
python benchmark.py compare a

# If satisfied with improvements:
python benchmark.py save-baseline a --force
```

### Example Output

```
2024-01-15 10:45:22 - INFO - [10:45:22] Starting benchmark for Pipeline A
2024-01-15 10:45:22 - INFO - [10:45:22] Step 1/4: Running pipeline(s)...
2024-01-15 10:45:22 - INFO - [10:45:22]   Executing pipeline A...
2024-01-15 10:46:15 - INFO - [10:46:15] ✓ Pipeline A completed: 12 models executed
2024-01-15 10:46:15 - INFO - [10:46:15] Step 2/4: Collecting performance metrics...
2024-01-15 10:46:22 - INFO - [10:46:22] ✓ Metrics collected for pipeline A
2024-01-15 10:46:22 - INFO - [10:46:22] Step 3/4: Verifying output correctness...
2024-01-15 10:46:25 - INFO - [10:46:25] ✓ Verification passed for pipeline A (12/12 models)
2024-01-15 10:46:25 - INFO - [10:46:25] Step 4/4: Saving results...
2024-01-15 10:46:25 - INFO - [10:46:25] ✓ Results saved for pipeline A
2024-01-15 10:46:25 - INFO - [10:46:25] ✓ Benchmark completed successfully for Pipeline A
```

### Example JSON Files

See `examples/baseline_example.json` and `examples/report_example.json` for realistic examples of baseline and report file formats.

## File Structure

```
snowflake/benchmark/
├── benchmark.py                 # Main CLI orchestrator
├── baseline.py                  # Baseline management
├── report.py                    # Comparison and reporting
├── metrics_collector.py          # Snowflake metrics collection
├── verify_output.py             # Output verification
├── run_pipeline.py              # dbt pipeline execution
├── benchmark_config.yml         # Pipeline definitions
├── requirements.txt             # Python dependencies
├── baselines/                   # Stored baselines (version-controlled)
│   ├── pipeline_a_baseline.json
│   ├── pipeline_b_baseline.json
│   └── pipeline_c_baseline.json
├── results/                     # Transient results (git-ignored)
│   ├── pipeline_a_result.json
│   ├── pipeline_b_result.json
│   └── pipeline_c_result.json
├── examples/                    # Example files
│   ├── baseline_example.json    # Example baseline structure
│   └── report_example.json      # Example report structure
└── README.md                    # This file
```

## Performance Optimization Tips

1. **Analyze Slow Models**: Focus on models with high execution_time first
2. **Reduce Bytes Scanned**: Add clustering keys to large tables
3. **Improve Joins**: Verify join conditions are selective
4. **Optimize Aggregations**: Use more efficient GROUP BY strategies
5. **Check Materializations**: Ensure ephemeral vs. table vs. view choices are optimal
6. **Monitor Warehouse**: Ensure sufficient warehouse size and no queueing

## Support and Contribution

For issues, questions, or contributions:

1. Check Troubleshooting section above
2. Review example files in `examples/` directory
3. Check dbt documentation: https://docs.getdbt.com/
4. Check Snowflake query optimization docs
