# End-to-End Validation Testing Guide

## Query-Time Measurement Feature - Complete Workflow Testing

This document provides a comprehensive manual testing procedure to validate the complete query-time measurement feature across all three benchmark pipelines (A, B, C).

---

## Overview of Feature

The query-time measurement feature adds a new measurement phase (Step 2.5) to benchmark execution:

1. **Step 2.5: Query-Time Performance Measurement**
   - Executes `SELECT COUNT(*)` queries on final models specified in `benchmark_config.yml`
   - Captures query execution metadata (query_id, execution_time_ms, bytes_scanned)
   - Collects metrics only for final models (not all executed models)
   - Provides graceful error handling for individual query failures

2. **Baseline Operations**
   - `save-baseline` now saves both `build_metrics` (DDL operations) and `query_metrics` (COUNT query execution)
   - Baseline validation detects old format and provides helpful error message

3. **Comparison Display**
   - `compare` command shows dual-section output
   - BUILD-TIME METRICS: Performance of dbt model creation
   - QUERY-TIME METRICS: Performance of COUNT(*) queries on final models
   - BUILD vs QUERY TRADEOFF ANALYSIS: Insights about the build vs query cost tradeoff

---

## Test Prerequisites

1. **Environment Setup**
   - Snowflake account credentials configured (via profiles.yml or environment variables)
   - dbt project properly configured
   - Access to SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY (requires ACCOUNTADMIN or equivalent)

2. **Configuration Verification**
   ```bash
   # Verify benchmark_config.yml has final_models defined for all pipelines
   cat snowflake/benchmark/benchmark_config.yml | grep -A 5 final_models
   ```

3. **Clean State**
   - Remove old result files: `rm -f snowflake/benchmark/results/pipeline_*.json`
   - Remove old baselines: `rm -f snowflake/benchmark/baselines/*_baseline.json`

---

## Test 1: Pipeline Execution Tests

### Test 1.1: Pipeline A Execution with Query-Time Measurement

**Objective**: Verify Pipeline A executes successfully with query-time measurement step

**Procedure**:
```bash
cd snowflake/benchmark
python benchmark.py run a --verbose
```

**Expected Output**:
```
[timestamp] - INFO - [timestamp] ✓ Starting benchmark for Pipeline A
[timestamp] - INFO - [timestamp] Step 1/4: Running pipeline(s)...
[timestamp] - INFO - [timestamp]   Executing pipeline A...
[timestamp] - INFO - [timestamp] ✓ Pipeline A completed: N models executed
[timestamp] - INFO - [timestamp] Step 2/4: Collecting performance metrics...
[timestamp] - INFO - [timestamp]   Collecting metrics for pipeline A...
[timestamp] - INFO - [timestamp] ✓ Metrics collected for pipeline A
[timestamp] - INFO - [timestamp] Step 2.5/4: Measuring query-time performance for final models...
[timestamp] - INFO - [timestamp]   Measuring query-time performance for 3 final models in pipeline A...
[timestamp] - INFO - [timestamp]   Executing COUNT(*) queries for 3 final models...
[timestamp] - INFO - [timestamp] ✓ COUNT query execution complete for pipeline A: 3 successful, 0 failed
[timestamp] - INFO - [timestamp]   Collecting metrics for 3 COUNT queries...
[timestamp] - INFO - [timestamp] ✓ Query-time metrics collected for pipeline A
[timestamp] - INFO - [timestamp] Step 3/4: Verifying output correctness...
[timestamp] - INFO - [timestamp] ✓ Verification passed for pipeline A (N/N models)
[timestamp] - INFO - [timestamp] Step 4/4: Saving results...
[timestamp] - INFO - [timestamp] ✓ Results saved for pipeline A
[timestamp] - INFO - [timestamp] ✓ Benchmark completed successfully for Pipeline A
```

**Verification Checklist**:
- [ ] No errors in output
- [ ] Step 2.5 executes successfully
- [ ] Final model count matches config (3 for Pipeline A)
- [ ] All COUNT queries execute successfully
- [ ] Results file created: `snowflake/benchmark/results/pipeline_a_result.json`

**Test Result**: ___________

---

### Test 1.2: Pipeline B Execution with Query-Time Measurement

**Objective**: Verify Pipeline B executes successfully with query-time measurement

**Procedure**:
```bash
cd snowflake/benchmark
python benchmark.py run b --verbose
```

**Expected Output**:
- Similar to Test 1.1, but for Pipeline B
- Should measure 3 final models (fact_trade_activity, fact_portfolio_pnl, report_daily_pnl)

**Verification Checklist**:
- [ ] No errors in output
- [ ] Step 2.5 completes with correct number of final models (3)
- [ ] All COUNT queries successful
- [ ] Results file created: `snowflake/benchmark/results/pipeline_b_result.json`

**Test Result**: ___________

---

### Test 1.3: Pipeline C Execution with Query-Time Measurement

**Objective**: Verify Pipeline C executes successfully with query-time measurement

**Procedure**:
```bash
cd snowflake/benchmark
python benchmark.py run c --verbose
```

**Expected Output**:
- Similar to Test 1.1, but for Pipeline C
- Should measure 4 final models (fact_fund_performance, fact_cashflow_waterfall, report_lp_quarterly, report_ic_dashboard)

**Verification Checklist**:
- [ ] No errors in output
- [ ] Step 2.5 completes with correct number of final models (4)
- [ ] All COUNT queries successful
- [ ] Results file created: `snowflake/benchmark/results/pipeline_c_result.json`

**Test Result**: ___________

---

## Test 2: Metrics Collection Tests

### Test 2.1: Verify Result JSON Structure Contains Both Metrics

**Objective**: Confirm result JSON files contain both build_metrics and query_metrics sections

**Procedure**:
```bash
# Examine Pipeline A result structure
python -c "
import json
with open('snowflake/benchmark/results/pipeline_a_result.json') as f:
    data = json.load(f)
    run_data = data.get('run_data', {})
    print('Top-level keys:', list(data.keys()))
    print('run_data keys:', list(run_data.keys()))
    print('build_metrics present:', 'metrics' in run_data)
    print('query_metrics present:', 'query_metrics' in run_data)
    if 'metrics' in run_data:
        print('build_metrics keys:', list(run_data['metrics'].keys())[:5])
    if 'query_metrics' in run_data:
        print('query_metrics keys:', list(run_data['query_metrics'].keys())[:5])
"
```

**Expected Output**:
```
Top-level keys: ['timestamp', 'pipeline_id', 'pipeline_name', 'run_data', 'verification_skipped']
run_data keys: ['pipeline_id', 'success', 'executed_models', 'fqn_models', 'start_timestamp', 'end_timestamp', 'error_message', 'metrics', 'query_metrics', 'verification']
build_metrics present: True
query_metrics present: True
build_metrics keys: ['pipeline_id', 'timestamp', 'total_execution_time_ms', 'total_bytes_scanned', 'total_rows_produced', ...]
query_metrics keys: ['pipeline_id', 'timestamp', 'total_execution_time_ms', 'total_bytes_scanned', 'total_rows_produced', ...]
```

**Verification Checklist**:
- [ ] Both 'metrics' and 'query_metrics' present in run_data
- [ ] build_metrics has: pipeline_id, timestamp, total_execution_time_ms, total_bytes_scanned, total_rows_produced
- [ ] query_metrics has: pipeline_id, timestamp, total_execution_time_ms, total_bytes_scanned, total_rows_produced
- [ ] Both metrics sections have model_metrics array with individual model data

**Test Result**: ___________

---

### Test 2.2: Verify COUNT Queries Target Only Final Models

**Objective**: Confirm that COUNT queries execute only on final_models specified in config, not all executed models

**Procedure**:
```bash
# Check query metrics model count vs final models count
python -c "
import json
import yaml
from pathlib import Path

# Load config to get final models count
with open('benchmark_config.yml') as f:
    config = yaml.safe_load(f)
    for pipeline in config['benchmarking']['pipelines']:
        if 'Pipeline A' in pipeline['name']:
            final_models_count = len(pipeline['final_models'])
            print(f'Pipeline A config final_models: {final_models_count}')
            print(f'  Models: {pipeline[\"final_models\"]}')

# Load result to check actual query metrics
with open('results/pipeline_a_result.json') as f:
    result = json.load(f)
    query_metrics = result['run_data'].get('query_metrics', {})
    model_metrics = query_metrics.get('model_metrics', [])
    print(f'Pipeline A actual query_metrics models: {len(model_metrics)}')
    for m in model_metrics:
        print(f'  - {m[\"model_name\"]}')
"
```

**Expected Output**:
```
Pipeline A config final_models: 3
  Models: ['fact_portfolio_summary', 'fact_portfolio_attribution', 'report_portfolio_overview']
Pipeline A actual query_metrics models: 3
  - fact_portfolio_summary
  - fact_portfolio_attribution
  - report_portfolio_overview
```

**Verification Checklist**:
- [ ] Number of models in query_metrics matches final_models in config
- [ ] Model names in query_metrics match final_models exactly (case-insensitive match)
- [ ] No extra models appear in query_metrics (only final models)
- [ ] For Pipeline B: 3 models (fact_trade_activity, fact_portfolio_pnl, report_daily_pnl)
- [ ] For Pipeline C: 4 models (fact_fund_performance, fact_cashflow_waterfall, report_lp_quarterly, report_ic_dashboard)

**Test Result**: ___________

---

### Test 2.3: Verify Query-Time Metrics Have Expected Values

**Objective**: Confirm query_metrics contain execution metrics with reasonable values

**Procedure**:
```bash
# Examine detailed query metrics
python -c "
import json
with open('results/pipeline_a_result.json') as f:
    result = json.load(f)
    query_metrics = result['run_data'].get('query_metrics', {})
    
    print('Query Metrics Summary:')
    print(f'  Total Execution Time: {query_metrics.get(\"total_execution_time_ms\")} ms')
    print(f'  Total Bytes Scanned: {query_metrics.get(\"total_bytes_scanned\")} bytes')
    print(f'  Total Rows Produced: {query_metrics.get(\"total_rows_produced\")} rows')
    
    print('\\nPer-Model Metrics:')
    for model in query_metrics.get('model_metrics', [])[:3]:
        print(f'  {model[\"model_name\"]}:')
        print(f'    - Execution Time: {model.get(\"total_execution_time_ms\")} ms')
        print(f'    - Bytes Scanned: {model.get(\"total_bytes_scanned\")} bytes')
        print(f'    - Rows Produced: {model.get(\"total_rows_produced\")} rows')
        print(f'    - Query Count: {model.get(\"query_count\")}')
"
```

**Expected Output**:
```
Query Metrics Summary:
  Total Execution Time: XXXX ms
  Total Bytes Scanned: XXXXXXXX bytes
  Total Rows Produced: XXXXX rows

Per-Model Metrics:
  fact_portfolio_summary:
    - Execution Time: XXX ms
    - Bytes Scanned: XXXXXXX bytes
    - Rows Produced: XXXXX rows
    - Query Count: 1
  ...
```

**Verification Checklist**:
- [ ] Total execution time is numeric and > 0
- [ ] Total bytes scanned is numeric (may be 0 for small tables)
- [ ] Total rows produced is numeric and >= 0
- [ ] Each model has exactly 1 query (query_count: 1)
- [ ] Totals match sum of per-model metrics
- [ ] Values are reasonable (not NaN or null)

**Test Result**: ___________

---

## Test 3: Baseline Operations Tests

### Test 3.1: Save Baseline for Pipeline A

**Objective**: Verify save-baseline command saves both metric sections

**Procedure**:
```bash
cd snowflake/benchmark
python benchmark.py save-baseline a
```

**Expected Output**:
```
[timestamp] - INFO - [timestamp] Saving baseline for Pipeline A
[timestamp] - INFO - [timestamp] Processing pipeline A...
[timestamp] - INFO - [timestamp]   Saving baseline for pipeline A...
[timestamp] - INFO - [timestamp] ✓ Baseline saved for pipeline A
[timestamp] - INFO - [timestamp] ✓ Baseline saved successfully for Pipeline A
```

**Verification Checklist**:
- [ ] No errors in output
- [ ] Baseline file created: `snowflake/benchmark/baselines/pipeline_a_baseline.json`
- [ ] File is valid JSON

**Test Result**: ___________

---

### Test 3.2: Verify Baseline Contains New Format

**Objective**: Confirm baseline JSON has both build_metrics and query_metrics sections

**Procedure**:
```bash
python -c "
import json
from pathlib import Path

baseline_file = Path('snowflake/benchmark/baselines/pipeline_a_baseline.json')
if not baseline_file.exists():
    print('ERROR: Baseline file not found')
    exit(1)

with open(baseline_file) as f:
    baseline = json.load(f)
    
print('Baseline Structure:')
print(f'  Top-level keys: {list(baseline.keys())}')
print(f'  Has pipeline_name: {\"pipeline_name\" in baseline}')
print(f'  Has build_metrics: {\"build_metrics\" in baseline}')
print(f'  Has query_metrics: {\"query_metrics\" in baseline}')
print(f'  Has fingerprints: {\"fingerprints\" in baseline}')
print(f'  Has metadata: {\"metadata\" in baseline}')

if 'build_metrics' in baseline:
    bm = baseline['build_metrics']
    print(f'\\nbuild_metrics keys: {list(bm.keys())[:5]}')
    
if 'query_metrics' in baseline:
    qm = baseline['query_metrics']
    print(f'query_metrics keys: {list(qm.keys())[:5]}')

if 'metadata' in baseline:
    meta = baseline['metadata']
    print(f'\\nmetadata keys: {list(meta.keys())}')
"
```

**Expected Output**:
```
Baseline Structure:
  Top-level keys: ['pipeline_name', 'build_metrics', 'query_metrics', 'fingerprints', 'metadata']
  Has pipeline_name: True
  Has build_metrics: True
  Has query_metrics: True
  Has fingerprints: True
  Has metadata: True

build_metrics keys: ['pipeline_id', 'timestamp', 'total_execution_time_ms', 'total_bytes_scanned', 'total_rows_produced']
query_metrics keys: ['pipeline_id', 'timestamp', 'total_execution_time_ms', 'total_bytes_scanned', 'total_rows_produced']

metadata keys: ['timestamp', 'git_commit', 'dbt_version', 'username']
```

**Verification Checklist**:
- [ ] Baseline is new format (has both build_metrics and query_metrics at top level)
- [ ] NOT old format (does not have single 'metrics' key)
- [ ] build_metrics section is populated
- [ ] query_metrics section is populated
- [ ] fingerprints array present (even if empty)
- [ ] metadata present with timestamp

**Test Result**: ___________

---

### Test 3.3: Save Baselines for All Pipelines

**Objective**: Verify save-baseline works for all three pipelines

**Procedure**:
```bash
cd snowflake/benchmark
python benchmark.py save-baseline all
```

**Expected Output**:
```
[timestamp] - INFO - Saving baseline for All Pipelines
[timestamp] - INFO - Processing pipeline A...
[timestamp] - INFO - ✓ Baseline saved for pipeline A
[timestamp] - INFO - Processing pipeline B...
[timestamp] - INFO - ✓ Baseline saved for pipeline B
[timestamp] - INFO - Processing pipeline C...
[timestamp] - INFO - ✓ Baseline saved for pipeline C
[timestamp] - INFO - ✓ Baseline saved successfully for All Pipelines
```

**Verification Checklist**:
- [ ] Three baseline files created:
  - `snowflake/benchmark/baselines/pipeline_a_baseline.json`
  - `snowflake/benchmark/baselines/pipeline_b_baseline.json`
  - `snowflake/benchmark/baselines/pipeline_c_baseline.json`
- [ ] All files are valid JSON with new format
- [ ] Each has build_metrics and query_metrics sections

**Test Result**: ___________

---

### Test 3.4: Test Baseline Loading

**Objective**: Verify baseline loading works without errors and validates format correctly

**Procedure**:
```bash
python -c "
import sys
sys.path.insert(0, 'snowflake/benchmark')
from baseline import load_baseline, validate_baseline

# Test load and validate
for pipeline_name in ['Pipeline A', 'Pipeline B', 'Pipeline C']:
    print(f'Testing {pipeline_name}...')
    baseline_data = load_baseline(pipeline_name)
    
    if baseline_data is None:
        print(f'  ERROR: Failed to load baseline')
        continue
    
    is_valid, message = validate_baseline(baseline_data)
    if is_valid:
        print(f'  ✓ Baseline loaded and validated')
        print(f'    - build_metrics: {list(baseline_data.get(\"build_metrics\", {}).keys())[:3]}')
        print(f'    - query_metrics: {list(baseline_data.get(\"query_metrics\", {}).keys())[:3]}')
    else:
        print(f'  ERROR: {message}')
"
```

**Expected Output**:
```
Testing Pipeline A...
  ✓ Baseline loaded and validated
    - build_metrics: ['pipeline_id', 'timestamp', 'total_execution_time_ms']
    - query_metrics: ['pipeline_id', 'timestamp', 'total_execution_time_ms']
Testing Pipeline B...
  ✓ Baseline loaded and validated
    - build_metrics: ['pipeline_id', 'timestamp', 'total_execution_time_ms']
    - query_metrics: ['pipeline_id', 'timestamp', 'total_execution_time_ms']
Testing Pipeline C...
  ✓ Baseline loaded and validated
    - build_metrics: ['pipeline_id', 'timestamp', 'total_execution_time_ms']
    - query_metrics: ['pipeline_id', 'timestamp', 'total_execution_time_ms']
```

**Verification Checklist**:
- [ ] All three baselines load successfully
- [ ] All pass validation
- [ ] build_metrics and query_metrics present in each
- [ ] No format errors

**Test Result**: ___________

---

## Test 4: Comparison Display Tests

### Test 4.1: Compare Pipeline A Against Baseline

**Objective**: Verify compare command displays dual-section metrics and tradeoff analysis

**Procedure**:
```bash
cd snowflake/benchmark
python benchmark.py compare a
```

**Expected Output**:
Should show:
1. BUILD-TIME METRICS section with comparisons
2. QUERY-TIME METRICS section with comparisons
3. BUILD vs QUERY TRADEOFF ANALYSIS section

Example format:
```
===========================================================================
Pipeline: Pipeline A
Timestamp: 2024-XX-XXTXX:XX:XXXZ
===========================================================================

BUILD-TIME METRICS:
Metric                         Baseline        Current         Change
───────────────────────────────────────────────────────────────────────
Build Execution Time               5000           5500       +10.0% ↑ REGRESSION
Build Bytes Scanned              100000         100000        +0.0% → NEUTRAL
Build Rows Produced               10000          10000        +0.0% → NEUTRAL

QUERY-TIME METRICS:
Metric                         Baseline        Current         Change
───────────────────────────────────────────────────────────────────────
Query Execution Time               2000           1200       -40.0% ✓ IMPROVED
Query Bytes Scanned             1000000         600000       -40.0% ✓ IMPROVED
Query Rows Produced               50000          50000        +0.0% → NEUTRAL

BUILD vs QUERY TRADEOFF ANALYSIS:
Build cost increased +10.0%, but query performance improved -40.0% (faster queries)
→ FAVORABLE: Higher build cost for faster queries

===========================================================================
```

**Verification Checklist**:
- [ ] Output shows all three sections (BUILD-TIME, QUERY-TIME, TRADEOFF)
- [ ] BUILD-TIME METRICS shows 3 metrics (Execution Time, Bytes Scanned, Rows Produced)
- [ ] QUERY-TIME METRICS shows 3 metrics (Execution Time, Bytes Scanned, Rows Produced)
- [ ] Each metric shows: Baseline value, Current value, Change %
- [ ] Status indicators present (✓ IMPROVED, ↑ REGRESSION, → NEUTRAL)
- [ ] Color coding applied (green for improvements, red for regressions)
- [ ] TRADEOFF ANALYSIS shows insight and recommendation
- [ ] No errors in output

**Test Result**: ___________

---

### Test 4.2: Compare All Pipelines

**Objective**: Verify compare works for all pipelines simultaneously

**Procedure**:
```bash
cd snowflake/benchmark
python benchmark.py compare all
```

**Expected Output**:
- Three separate comparison reports (one for each pipeline)
- Each with dual-section display
- All complete successfully

**Verification Checklist**:
- [ ] Three pipeline comparisons shown (A, B, C)
- [ ] Each shows BUILD-TIME METRICS section
- [ ] Each shows QUERY-TIME METRICS section
- [ ] Each shows BUILD vs QUERY TRADEOFF ANALYSIS
- [ ] No errors
- [ ] Command exits with code 0

**Test Result**: ___________

---

### Test 4.3: Verify Comparison Values Are Calculated Correctly

**Objective**: Ensure percentage changes are computed accurately

**Procedure**:
```bash
python -c "
import json
from decimal import Decimal

# Load baseline and current metrics
with open('results/pipeline_a_result.json') as f:
    current = json.load(f)
    
with open('baselines/pipeline_a_baseline.json') as f:
    baseline = json.load(f)

# Extract build metrics
baseline_build = baseline.get('build_metrics', {})
current_build = current['run_data'].get('metrics', {})

# Calculate build execution time change manually
baseline_exec = baseline_build.get('total_execution_time_ms', 0)
current_exec = current_build.get('total_execution_time_ms', 0)

if baseline_exec > 0:
    change_pct = ((current_exec - baseline_exec) / baseline_exec) * 100
    print(f'Build Execution Time Change:')
    print(f'  Baseline: {baseline_exec}')
    print(f'  Current: {current_exec}')
    print(f'  Calculated Change: {change_pct:.1f}%')

# Extract query metrics
baseline_query = baseline.get('query_metrics', {})
current_query = current['run_data'].get('query_metrics', {})

baseline_query_exec = baseline_query.get('total_execution_time_ms', 0)
current_query_exec = current_query.get('total_execution_time_ms', 0)

if baseline_query_exec > 0:
    change_pct = ((current_query_exec - baseline_query_exec) / baseline_query_exec) * 100
    print(f'\\nQuery Execution Time Change:')
    print(f'  Baseline: {baseline_query_exec}')
    print(f'  Current: {current_query_exec}')
    print(f'  Calculated Change: {change_pct:.1f}%')
"
```

**Expected Output**:
```
Build Execution Time Change:
  Baseline: 5000
  Current: 5500
  Calculated Change: +10.0%

Query Execution Time Change:
  Baseline: 2000
  Current: 1200
  Calculated Change: -40.0%
```

**Verification Checklist**:
- [ ] Build metrics calculation correct
- [ ] Query metrics calculation correct
- [ ] Values in comparison output match manual calculations
- [ ] Percentage signs and directions correct (+ for increase, - for decrease)

**Test Result**: ___________

---

## Test 5: Error Handling Tests

### Test 5.1: Graceful Error Handling for Failed COUNT Queries

**Objective**: Verify system handles COUNT query failures gracefully without crashing

**Procedure**:

1. First, run the benchmark normally to establish baselines:
```bash
cd snowflake/benchmark
python benchmark.py run a
```

2. Simulate a query failure by temporarily dropping one of the final models:
```sql
-- Connect to Snowflake and execute:
DROP VIEW IF EXISTS fact_portfolio_summary;
```

3. Run benchmark again:
```bash
python benchmark.py run a --verbose
```

4. Restore the model:
```bash
cd snowflake
dbt run -s fact_portfolio_summary
```

**Expected Behavior**:
- Step 2.5 shows some failed COUNT queries
- Logs should show:
  ```
  [timestamp] - WARNING - Failed to execute COUNT query for fact_portfolio_summary: ...
  [timestamp] - INFO - COUNT query execution complete for pipeline A: 2 successful, 1 failed
  ```
- The failed query does NOT cause the entire benchmark to fail
- Benchmark continues with available metrics
- Results are saved with query_metrics containing only successful queries

**Verification Checklist**:
- [ ] Benchmark does NOT crash when COUNT query fails
- [ ] Failed query logged as WARNING, not ERROR
- [ ] Successful count and failed count shown correctly
- [ ] Benchmark completes successfully (exit code 0)
- [ ] Results saved with query_metrics containing successful models only

**Test Result**: ___________

---

### Test 5.2: Graceful Handling of Missing Final Models

**Objective**: Verify system handles case where final_models don't exist in FQN list

**Procedure**:

1. Temporarily modify benchmark_config.yml to reference non-existent models:
```yaml
# In pipelines section for Pipeline A:
final_models:
  - "fact_portfolio_summary"
  - "nonexistent_model_xyz"
  - "report_portfolio_overview"
```

2. Run benchmark:
```bash
python benchmark.py run a
```

3. Revert benchmark_config.yml to original

**Expected Behavior**:
- Step 2.5 logs:
  ```
  [timestamp] - INFO - Measuring query-time performance for 3 final models in pipeline A...
  [timestamp] - WARNING - No matching final models found in FQN list for pipeline A
  [timestamp] - INFO - query_metrics set to empty dict
  ```
- Benchmark continues successfully
- Results saved with empty query_metrics

**Verification Checklist**:
- [ ] Missing models handled gracefully
- [ ] Warning logged but no error
- [ ] Benchmark completes successfully
- [ ] query_metrics is empty dict instead of partial data

**Test Result**: ___________

---

### Test 5.3: Handling Baseline Format Errors

**Objective**: Verify system detects and reports old baseline format

**Procedure**:

1. Manually create an old-format baseline file:
```bash
python -c "
import json
from pathlib import Path

# Create old format baseline
old_format = {
    'pipeline_name': 'Pipeline Test',
    'metrics': {  # OLD FORMAT: single 'metrics' key
        'total_execution_time_ms': 5000,
        'total_bytes_scanned': 100000,
    },
    'fingerprints': [],
    'metadata': {'timestamp': '2024-01-01T00:00:00Z'}
}

Path('snowflake/benchmark/baselines/pipeline_test_baseline.json').write_text(
    json.dumps(old_format, indent=2)
)
"
```

2. Try to compare using the old format:
```bash
# This should fail with informative error
python -c "
import sys
sys.path.insert(0, 'snowflake/benchmark')
from baseline import load_baseline, validate_baseline

try:
    baseline = load_baseline('Pipeline Test')
    is_valid, msg = validate_baseline(baseline)
    if not is_valid:
        print(f'Validation failed (expected): {msg}')
except ValueError as e:
    print(f'ValueError caught (expected): {e}')
"
```

3. Clean up:
```bash
rm snowflake/benchmark/baselines/pipeline_test_baseline.json
```

**Expected Behavior**:
- Old format baseline raises ValueError or validation error
- Error message indicates old format and suggests regenerating:
  ```
  ValueError: Baseline file uses old format. Please regenerate baseline with updated benchmark.py
  ```

**Verification Checklist**:
- [ ] Old format is detected
- [ ] Helpful error message provided
- [ ] User knows to regenerate baseline
- [ ] No silent data corruption

**Test Result**: ___________

---

## Test 6: View vs Table Performance Comparison

### Test 6.1: Verify Query-Time Metrics Show Meaningful Differences

**Objective**: Confirm that views (expensive to query) vs tables (cheap to query) show different query-time metrics

**Note**: This test requires pipelines with both views and tables in final_models. Review your data models to identify:
- **Views** in final_models: Should show high bytes_scanned and execution_time in query_metrics
- **Tables** in final_models: Should show low bytes_scanned and execution_time in query_metrics

**Procedure**:

1. Run benchmarks and compare query metrics:
```bash
python -c "
import json

with open('results/pipeline_a_result.json') as f:
    result = json.load(f)
    query_metrics = result['run_data'].get('query_metrics', {})
    
    print('Query-Time Metrics by Model Type:')
    for model in query_metrics.get('model_metrics', []):
        name = model['model_name']
        exec_time = model.get('total_execution_time_ms', 0)
        bytes = model.get('total_bytes_scanned', 0)
        rows = model.get('total_rows_produced', 0)
        
        print(f'{name}:')
        print(f'  Execution Time: {exec_time} ms')
        print(f'  Bytes Scanned: {bytes} bytes')
        print(f'  Rows Produced: {rows}')
"
```

2. Compare with build metrics:
```bash
python -c "
import json

with open('results/pipeline_a_result.json') as f:
    result = json.load(f)
    build = result['run_data'].get('metrics', {})
    query = result['run_data'].get('query_metrics', {})
    
    print('Build vs Query Comparison:')
    print(f'Build Bytes Scanned: {build.get(\"total_bytes_scanned\", 0)}')
    print(f'Query Bytes Scanned: {query.get(\"total_bytes_scanned\", 0)}')
    print(f'Ratio (Query/Build): {query.get(\"total_bytes_scanned\", 0) / max(1, build.get(\"total_bytes_scanned\", 1)):.1f}x')
"
```

**Expected Patterns**:
- **Views**: Query bytes_scanned >> Build bytes_scanned (many times larger)
- **Tables**: Query bytes_scanned ≈ Build bytes_scanned (similar magnitude)
- **Overall**: Query metrics should demonstrate the view vs table tradeoff

**Verification Checklist**:
- [ ] Query metrics populated with expected differences
- [ ] Views show higher query-time metrics than tables
- [ ] Data makes sense relative to model types
- [ ] Tradeoff analysis correctly interprets the data

**Test Result**: ___________

---

## Test 7: Integration Verification

### Test 7.1: Complete End-to-End Workflow

**Objective**: Verify entire workflow (run → save-baseline → compare) works together

**Procedure**:
```bash
cd snowflake/benchmark

# Step 1: Run benchmark for Pipeline A (fresh)
echo "Running Pipeline A..."
python benchmark.py run a

# Step 2: Save as baseline (first time)
echo "Saving baseline for Pipeline A..."
python benchmark.py save-baseline a

# Step 3: Run again (second time)
echo "Running Pipeline A (second time)..."
python benchmark.py run a

# Step 4: Compare against baseline
echo "Comparing against baseline..."
python benchmark.py compare a
```

**Expected Behavior**:
- All commands succeed with exit code 0
- Each step produces expected output
- Comparison shows meaningful metrics
- No errors or warnings

**Verification Checklist**:
- [ ] Run 1 completes successfully
- [ ] Baseline saved successfully with both metric sections
- [ ] Run 2 completes successfully
- [ ] Compare shows dual-section output
- [ ] All four commands exit with code 0
- [ ] No data corruption or format issues

**Test Result**: ___________

---

### Test 7.2: Concurrent Benchmark Execution

**Objective**: Verify system handles running all pipelines simultaneously

**Procedure**:
```bash
cd snowflake/benchmark

# Run all pipelines
echo "Running all pipelines..."
python benchmark.py run all

# Save baselines for all
echo "Saving baselines..."
python benchmark.py save-baseline all

# Run again
echo "Running all pipelines (second time)..."
python benchmark.py run all

# Compare all
echo "Comparing all pipelines..."
python benchmark.py compare all
```

**Expected Behavior**:
- All commands succeed
- All three pipelines process correctly
- No resource conflicts or data mixing

**Verification Checklist**:
- [ ] All three pipelines run successfully
- [ ] Results for A, B, C all saved separately
- [ ] Baselines for A, B, C all saved separately
- [ ] Comparison reports for A, B, C all shown
- [ ] No cross-pipeline data contamination

**Test Result**: ___________

---

## Test 8: Log Output Verification

### Test 8.1: Verify Log Messages Show Query-Time Measurement Step

**Objective**: Confirm Step 2.5 logging indicates query-time measurement is executing

**Procedure**:
```bash
cd snowflake/benchmark
python benchmark.py run a 2>&1 | grep -A 10 "Step 2.5"
```

**Expected Output**:
```
[timestamp] Step 2.5/4: Measuring query-time performance for final models...
[timestamp]   Measuring query-time performance for 3 final models in pipeline A...
[timestamp]   Executing COUNT(*) queries for 3 final models...
[timestamp]   Collecting metrics for 3 COUNT queries...
[timestamp] ✓ Query-time metrics collected for pipeline A
```

**Verification Checklist**:
- [ ] "Step 2.5/4" message present
- [ ] Final model count accurate
- [ ] COUNT query execution message present
- [ ] Query-time metrics collection confirmed
- [ ] Success indicator (✓) shown

**Test Result**: ___________

---

## Summary Checklist

Use this checklist to track overall test completion:

### Pipeline Execution
- [ ] Test 1.1: Pipeline A execution passes
- [ ] Test 1.2: Pipeline B execution passes
- [ ] Test 1.3: Pipeline C execution passes

### Metrics Collection
- [ ] Test 2.1: Result JSON structure correct
- [ ] Test 2.2: COUNT queries target final models only
- [ ] Test 2.3: Query metrics have expected values

### Baseline Operations
- [ ] Test 3.1: Save baseline succeeds
- [ ] Test 3.2: Baseline new format verified
- [ ] Test 3.3: All three pipelines save baselines
- [ ] Test 3.4: Baseline loading works

### Comparison Display
- [ ] Test 4.1: Pipeline A comparison shows dual-sections
- [ ] Test 4.2: All pipelines comparison works
- [ ] Test 4.3: Comparison values calculated correctly

### Error Handling
- [ ] Test 5.1: Failed COUNT queries handled gracefully
- [ ] Test 5.2: Missing final models handled gracefully
- [ ] Test 5.3: Baseline format errors detected

### Performance Validation
- [ ] Test 6.1: View vs table differences shown

### Integration
- [ ] Test 7.1: Complete end-to-end workflow works
- [ ] Test 7.2: Concurrent execution works

### Logging
- [ ] Test 8.1: Log output shows Step 2.5

---

## Failure Investigation Guide

If any test fails, use these debugging steps:

1. **Check Snowflake Connection**:
   ```bash
   python -c "
   from snowflake.connector import connect
   # Verify credentials work
   ```

2. **Review Logs**:
   ```bash
   python benchmark.py run a 2>&1 | tee debug.log
   # Check for specific error messages
   ```

3. **Validate JSON Files**:
   ```bash
   python -m json.tool snowflake/benchmark/results/pipeline_a_result.json
   python -m json.tool snowflake/benchmark/baselines/pipeline_a_baseline.json
   ```

4. **Check Query History**:
   ```sql
   SELECT QUERY_TEXT, QUERY_ID, ERROR_CODE, ERROR_MESSAGE 
   FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
   WHERE QUERY_TEXT LIKE '%COUNT%'
   AND START_TIME > CURRENT_TIMESTAMP - INTERVAL '1 hour'
   ORDER BY START_TIME DESC;
   ```

5. **Verify Configuration**:
   ```bash
   python -c "
   import yaml
   with open('snowflake/benchmark/benchmark_config.yml') as f:
       config = yaml.safe_load(f)
       print(yaml.dump(config, default_flow_style=False))
   "
   ```

---

## Document Signing

**Tested By**: ___________________

**Test Date**: ___________________

**Overall Result**: [ ] PASS [ ] FAIL

**Notes**:
____________________________________________________________________
____________________________________________________________________
____________________________________________________________________

