#!/usr/bin/env python3
"""
Benchmark System CLI Orchestrator

Main entry point for the benchmark system. Orchestrates pipeline execution,
metrics collection, output verification, and baseline management through
an intuitive command-line interface.

Commands:
    run <pipeline>          - Execute a pipeline and collect metrics
    save-baseline <pipeline> - Save current results as baseline
    compare <pipeline>      - Compare current results against baseline

Usage:
    python benchmark.py run a --verbose
    python benchmark.py save-baseline all --force
    python benchmark.py compare b
"""

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, List

import yaml

# Import benchmark modules
from run_pipeline import run_pipeline, PipelineRunResult
from metrics_collector import MetricsCollector
from verify_output import OutputVerifier
import baseline
import report


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Console handler (will be configured based on verbose flag)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# Exit codes
EXIT_SUCCESS = 0
EXIT_BENCHMARK_FAILED = 1
EXIT_VERIFICATION_FAILED = 2
EXIT_INVALID_ARGUMENTS = 3


def get_results_dir() -> Path:
    """Get the results directory path."""
    benchmark_dir = Path(__file__).parent
    results_dir = benchmark_dir / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)
    return results_dir


def get_result_file_path(pipeline_id: str) -> Path:
    """Get the path to a result file for a pipeline."""
    results_dir = get_results_dir()
    # Normalize pipeline_id to lowercase for filename
    pipeline_lower = pipeline_id.lower()
    return results_dir / f"pipeline_{pipeline_lower}_result.json"


def normalize_pipeline_id(pipeline_arg: str) -> str:
    """
    Normalize pipeline argument (a, b, c, all) to internal format (A, B, C, all).
    
    Args:
        pipeline_arg: User-provided pipeline argument (lowercase expected)
        
    Returns:
        Normalized pipeline ID
        
    Raises:
        ValueError: If pipeline_arg is invalid
    """
    pipeline_arg = pipeline_arg.lower().strip()
    
    if pipeline_arg in ('a', 'b', 'c', 'all'):
        if pipeline_arg == 'all':
            return 'all'
        return pipeline_arg.upper()
    
    raise ValueError(f"Invalid pipeline: {pipeline_arg}. Must be 'a', 'b', 'c', or 'all'")


def get_pipeline_name(pipeline_id: str) -> str:
    """Get human-readable pipeline name from ID."""
    if pipeline_id == 'A':
        return 'Pipeline A'
    elif pipeline_id == 'B':
        return 'Pipeline B'
    elif pipeline_id == 'C':
        return 'Pipeline C'
    elif pipeline_id == 'all':
        return 'All Pipelines'
    return f'Pipeline {pipeline_id}'


def save_result(pipeline_id: str, result_data: Dict[str, Any]) -> bool:
    """
    Save result data to JSON file.
    
    Args:
        pipeline_id: Pipeline identifier
        result_data: Result data dictionary
        
    Returns:
        True if successful, False otherwise
    """
    try:
        file_path = get_result_file_path(pipeline_id)
        with open(file_path, 'w') as f:
            json.dump(result_data, f, indent=2, default=str)
        logger.info(f"Saved results to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to save results: {e}")
        return False


def load_result(pipeline_id: str) -> Optional[Dict[str, Any]]:
    """
    Load result data from JSON file.
    
    Args:
        pipeline_id: Pipeline identifier
        
    Returns:
        Result data dictionary or None if not found
    """
    try:
        file_path = get_result_file_path(pipeline_id)
        if not file_path.exists():
            logger.warning(f"Result file not found: {file_path}")
            return None
        
        with open(file_path, 'r') as f:
            data = json.load(f)
        logger.debug(f"Loaded results from {file_path}")
        return data
    except Exception as e:
        logger.error(f"Failed to load results: {e}")
        return None


def print_progress(message: str, level: str = "info") -> None:
    """Print progress message with timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if level == "info":
        logger.info(f"[{timestamp}] {message}")
    elif level == "success":
        logger.info(f"[{timestamp}] ✓ {message}")
    elif level == "warning":
        logger.warning(f"[{timestamp}] ⚠ {message}")
    elif level == "error":
        logger.error(f"[{timestamp}] ✗ {message}")


def load_benchmark_config() -> Dict[str, Any]:
    """
    Load benchmark configuration from benchmark_config.yml.
    
    Returns:
        Dictionary with benchmarking configuration (pipelines, final_models, etc.)
    """
    benchmark_dir = Path(__file__).parent
    config_file = benchmark_dir / 'benchmark_config.yml'
    
    if not config_file.exists():
        logger.warning(f"benchmark_config.yml not found at {config_file}")
        return {}
    
    try:
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
            return config if config else {}
    except Exception as e:
        logger.error(f"Error loading benchmark config: {e}")
        return {}


def get_final_models_for_pipeline(pipeline_id: str, config: Dict[str, Any]) -> List[str]:
    """
    Get the list of final_models for a given pipeline from the config.
    
    Args:
        pipeline_id: Pipeline identifier (A, B, C)
        config: Benchmark configuration dictionary
        
    Returns:
        List of final model names (lowercase) for the pipeline
    """
    pipelines = config.get('pipelines', [])
    
    for pipeline in pipelines:
        # Match pipeline by name (e.g., "Pipeline A", "Pipeline B", etc.)
        name = pipeline.get('name', '')
        if pipeline_id.upper() in name.upper():
            return pipeline.get('final_models', [])
    
    return []


def filter_fqn_models_by_final_models(
    fqn_models: List[str],
    final_models: List[str]
) -> List[Dict[str, str]]:
    """
    Filter FQN models to match final_models with case-insensitive matching.
    
    Args:
        fqn_models: List of fully qualified model names (e.g., 'DBT_DEMO.DEV.FACT_PORTFOLIO_SUMMARY')
        final_models: List of final model names from config (lowercase, e.g., 'fact_portfolio_summary')
        
    Returns:
        List of dicts with 'model_name' and 'fqn' keys for matched models
    """
    # Create a mapping of lowercase model names to FQNs
    fqn_lower_map = {fqn.split('.')[-1].lower(): fqn for fqn in fqn_models}
    
    # Filter to only final models
    final_models_lower = [m.lower() for m in final_models]
    
    result = []
    for final_model in final_models_lower:
        if final_model in fqn_lower_map:
            fqn = fqn_lower_map[final_model]
            result.append({
                'model_name': final_model,
                'fqn': fqn,
            })
    
    return result


def run_command(args) -> int:
    """
    Execute the 'run' command: run pipeline, collect metrics, verify, and save results.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code
    """
    try:
        pipeline_id = normalize_pipeline_id(args.pipeline)
    except ValueError as e:
        print_progress(str(e), "error")
        return EXIT_INVALID_ARGUMENTS
    
    pipeline_name = get_pipeline_name(pipeline_id)
    print_progress(f"Starting benchmark for {pipeline_name}", "info")
    
    # Step 1: Run pipeline(s)
    print_progress("Step 1/4: Running pipeline(s)...", "info")
    
    pipelines_to_run = [pipeline_id] if pipeline_id != 'all' else ['A', 'B', 'C']
    all_results = {}
    
    for pid in pipelines_to_run:
        print_progress(f"  Executing pipeline {pid}...", "info")
        
        try:
            run_result = run_pipeline(pid, compile_only=False)
            
            if not run_result.success:
                print_progress(
                    f"Pipeline {pid} execution failed: {run_result.error_message}",
                    "error"
                )
                return EXIT_BENCHMARK_FAILED
            
            all_results[pid] = {
                'pipeline_id': run_result.pipeline_id,
                'success': run_result.success,
                'executed_models': run_result.executed_models,
                'fqn_models': run_result.fqn_models,
                'start_timestamp': run_result.start_timestamp,
                'end_timestamp': run_result.end_timestamp,
                'error_message': run_result.error_message,
            }
            
            print_progress(f"  Pipeline {pid} completed: {len(run_result.executed_models)} models executed", "success")
        
        except Exception as e:
            print_progress(f"Error running pipeline {pid}: {e}", "error")
            return EXIT_BENCHMARK_FAILED
    
    # Step 2: Collect metrics (wait for query history to populate)
    import time
    print_progress("Step 2/4: Collecting performance metrics (waiting 5s for query history)...", "info")
    time.sleep(5)

    metrics_collector = MetricsCollector()
    
    for pid in pipelines_to_run:
        try:
            run_data = all_results[pid]
            print_progress(f"  Collecting metrics for pipeline {pid}...", "info")
            
            metrics = metrics_collector.collect_metrics(
                pipeline_id=pid,
                executed_models=run_data['executed_models'],
                fqn_models=run_data['fqn_models'],
                start_timestamp=run_data['start_timestamp'],
                end_timestamp=run_data['end_timestamp'],
            )
            
            all_results[pid]['metrics'] = metrics
            print_progress(f"  Metrics collected for pipeline {pid}", "success")
        
        except Exception as e:
            print_progress(f"Error collecting metrics for pipeline {pid}: {e}", "error")
            return EXIT_BENCHMARK_FAILED
    
    # Step 2.5: Measure query-time performance for final models
    print_progress("Step 2.5/4: Measuring query-time performance for final models...", "info")
    
    # Load benchmark config
    benchmark_config = load_benchmark_config()
    
    for pid in pipelines_to_run:
        try:
            run_data = all_results[pid]
            
            # Get final models for this pipeline from config
            final_models = get_final_models_for_pipeline(pid, benchmark_config)
            
            if not final_models:
                print_progress(f"  No final models configured for pipeline {pid}, skipping query-time measurement", "warning")
                all_results[pid]['query_metrics'] = {}
                continue
            
            print_progress(f"  Measuring query-time performance for {len(final_models)} final models in pipeline {pid}...", "info")
            
            # Filter FQN models to only include final models (case-insensitive matching)
            filtered_models = filter_fqn_models_by_final_models(
                run_data['fqn_models'],
                final_models
            )
            
            if not filtered_models:
                print_progress(f"  No matching final models found in FQN list for pipeline {pid}", "warning")
                all_results[pid]['query_metrics'] = {}
                continue
            
            print_progress(f"  Executing COUNT(*) queries for {len(filtered_models)} final models...", "info")
            
            # Execute COUNT queries on final models
            sf_conn = metrics_collector.sf_conn
            if not sf_conn.connect():
                print_progress(f"Error connecting to Snowflake for query execution in pipeline {pid}", "error")
                return EXIT_BENCHMARK_FAILED
            
            try:
                query_execution_result = sf_conn.execute_count_queries(filtered_models)
                
                # Log execution results
                successful_count = query_execution_result.get('total_successful', 0)
                failed_count = query_execution_result.get('total_failed', 0)
                print_progress(
                    f"  COUNT query execution complete for pipeline {pid}: {successful_count} successful, {failed_count} failed",
                    "success" if failed_count == 0 else "warning"
                )
                
                # Get successful model results (each contains query_id, model_name, fqn, row_count)
                successful_models = query_execution_result.get('successful', [])

                if successful_models:
                    # Wait briefly for query history to populate
                    import time
                    time.sleep(3)

                    # Collect metrics by looking up query IDs directly in QUERY_HISTORY
                    print_progress(f"  Collecting query-time metrics for {len(successful_models)} models...", "info")

                    query_metrics = metrics_collector.collect_metrics_by_query_ids(
                        pipeline_id=pid,
                        query_results=successful_models,
                    )

                    all_results[pid]['query_metrics'] = query_metrics
                    print_progress(f"  Query-time metrics collected for pipeline {pid}", "success")
                else:
                    print_progress(f"  No successful COUNT queries for pipeline {pid}", "warning")
                    all_results[pid]['query_metrics'] = {}
            
            finally:
                sf_conn.close()
        
        except Exception as e:
            print_progress(f"Error measuring query-time performance for pipeline {pid}: {e}", "error")
            return EXIT_BENCHMARK_FAILED
    
    # Step 3: Verify output (if not --no-verify)
    verification_results = {}
    
    if not args.no_verify:
        print_progress("Step 3/4: Verifying output correctness...", "info")
        
        verifier = OutputVerifier()
        
        for pid in pipelines_to_run:
            try:
                run_data = all_results[pid]
                print_progress(f"  Verifying models for pipeline {pid}...", "info")
                
                # Load baseline fingerprints (if they exist)
                baseline_data = baseline.load_baseline(get_pipeline_name(pid))
                baseline_fingerprints = []
                if baseline_data:
                    baseline_fingerprints = baseline_data.get('fingerprints', [])
                
                # Verify models
                verification = verifier.verify_models(
                    pipeline_id=pid,
                    model_fqns=run_data['fqn_models'],
                    baseline_fingerprints=baseline_fingerprints,
                )
                
                verification_results[pid] = verification
                
                # Check verification result
                passed = verification.get('passed_models', 0)
                total = verification.get('total_models', 0)
                
                if passed == total:
                    print_progress(f"  Verification passed for pipeline {pid} ({passed}/{total} models)", "success")
                else:
                    print_progress(
                        f"  Verification completed for pipeline {pid} ({passed}/{total} models passed)",
                        "warning" if passed > 0 else "error"
                    )
                    if passed < total:
                        return EXIT_VERIFICATION_FAILED
                
                all_results[pid]['verification'] = verification
            
            except Exception as e:
                print_progress(f"Error verifying pipeline {pid}: {e}", "error")
                return EXIT_VERIFICATION_FAILED
    else:
        print_progress("Step 3/4: Skipped output verification (--no-verify flag set)", "info")
    
    # Step 4: Save results
    print_progress("Step 4/4: Saving results...", "info")
    
    for pid in pipelines_to_run:
        try:
            result_data = {
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'pipeline_id': pid,
                'pipeline_name': get_pipeline_name(pid),
                'run_data': all_results[pid],
                'verification_skipped': args.no_verify,
            }
            
            if not save_result(pid, result_data):
                print_progress(f"Failed to save results for pipeline {pid}", "error")
                return EXIT_BENCHMARK_FAILED
            
            print_progress(f"  Results saved for pipeline {pid}", "success")
        
        except Exception as e:
            print_progress(f"Error saving results for pipeline {pid}: {e}", "error")
            return EXIT_BENCHMARK_FAILED
    
    print_progress(f"Benchmark completed successfully for {pipeline_name}", "success")
    return EXIT_SUCCESS


def save_baseline_command(args) -> int:
    """
    Execute the 'save-baseline' command: load results and save as baseline.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Exit code
    """
    try:
        pipeline_id = normalize_pipeline_id(args.pipeline)
    except ValueError as e:
        print_progress(str(e), "error")
        return EXIT_INVALID_ARGUMENTS
    
    pipeline_name = get_pipeline_name(pipeline_id)
    print_progress(f"Saving baseline for {pipeline_name}", "info")
    
    pipelines_to_save = [pipeline_id] if pipeline_id != 'all' else ['A', 'B', 'C']
    
    for pid in pipelines_to_save:
        pname = get_pipeline_name(pid)
        print_progress(f"Processing pipeline {pid}...", "info")
        
        try:
            # Load latest results
            result_data = load_result(pid)
            
            if not result_data:
                print_progress(
                    f"No results found for pipeline {pid}. Run benchmark first.",
                    "error"
                )
                return EXIT_BENCHMARK_FAILED
            
            run_data = result_data.get('run_data', {})
            
            # Extract both build-time and query-time metrics
            build_metrics = run_data.get('metrics', {})
            query_metrics = run_data.get('query_metrics', {})
            
            if not build_metrics:
                print_progress(
                    f"No build metrics found for pipeline {pid}. Run benchmark first.",
                    "error"
                )
                return EXIT_BENCHMARK_FAILED
            
            # Get fingerprints from verification results (if available)
            fingerprints = []
            verification = run_data.get('verification', {})
            
            if verification:
                # Extract fingerprints from verification results
                results = verification.get('results', [])
                for result in results:
                    fingerprints.append({
                        'model_name': result.get('model_name', ''),
                        'fqn': result.get('fqn', ''),
                        'row_count': result.get('current_row_count', 0),
                        'content_hash': result.get('current_hash'),
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                    })
            
            # Check if baseline already exists (unless --force)
            existing_baseline = None
            try:
                existing_baseline = baseline.load_baseline(pname)
            except ValueError as e:
                # Old format detected - treat as existing baseline that needs overwrite
                if args.force:
                    print_progress(f"  Old format baseline detected, will overwrite with --force", "info")
                    existing_baseline = True  # Signal that it exists but is old format
                else:
                    print_progress(f"Baseline format error: {e}", "error")
                    return EXIT_BENCHMARK_FAILED

            if existing_baseline and not args.force:
                print_progress(
                    f"Baseline already exists for {pname}. Use --force to overwrite.",
                    "warning"
                )
                return EXIT_INVALID_ARGUMENTS
            
            # Save baseline with both build and query metrics
            print_progress(f"  Saving baseline for pipeline {pid}...", "info")
            success = baseline.save_baseline(
                pipeline_name=pname,
                build_metrics=build_metrics,
                query_metrics=query_metrics,
                fingerprints=fingerprints,
            )
            
            if not success:
                print_progress(f"Failed to save baseline for pipeline {pid}", "error")
                return EXIT_BENCHMARK_FAILED
            
            print_progress(f"  Baseline saved for pipeline {pid}", "success")
        
        except Exception as e:
            print_progress(f"Error saving baseline for pipeline {pid}: {e}", "error")
            return EXIT_BENCHMARK_FAILED
    
    print_progress(f"Baseline saved successfully for {pipeline_name}", "success")
    return EXIT_SUCCESS


def compare_command(args) -> int:
    """
    Execute the 'compare' command: compare current results against baseline.

    Loads current benchmark results and saved baselines, then produces a
    detailed comparison report with:
    - BUILD-TIME METRICS comparison (DDL registration cost)
    - QUERY-TIME METRICS comparison (actual query performance)
    - BUILD vs QUERY TRADEOFF ANALYSIS
    - Per-model detail breakdown (with --detail flag)
    - JSON report files saved to results/reports/

    Args:
        args: Parsed command line arguments

    Returns:
        Exit code
    """
    try:
        pipeline_id = normalize_pipeline_id(args.pipeline)
    except ValueError as e:
        print_progress(str(e), "error")
        return EXIT_INVALID_ARGUMENTS

    pipeline_name = get_pipeline_name(pipeline_id)
    print_progress(f"Comparing results for {pipeline_name}", "info")

    pipelines_to_compare = [pipeline_id] if pipeline_id != 'all' else ['A', 'B', 'C']
    use_colors = not getattr(args, 'no_color', False)
    show_detail = getattr(args, 'detail', False)
    all_comparisons = []
    has_regressions = False

    for pid in pipelines_to_compare:
        pname = get_pipeline_name(pid)
        print_progress(f"Comparing pipeline {pid}...", "info")

        try:
            # Load current results
            result_data = load_result(pid)
            if not result_data:
                print_progress(
                    f"No results found for pipeline {pid}. Run benchmark first.",
                    "error"
                )
                return EXIT_BENCHMARK_FAILED

            current_metrics = result_data.get('run_data', {}).get('metrics', {})
            current_query_metrics = result_data.get('run_data', {}).get('query_metrics', {})

            # Load baseline
            try:
                baseline_data = baseline.load_baseline(pname)
            except ValueError as e:
                print_progress(f"Baseline format error: {e}", "error")
                return EXIT_BENCHMARK_FAILED

            if not baseline_data:
                print_progress(
                    f"No baseline found for {pname}. Save baseline first.",
                    "error"
                )
                return EXIT_BENCHMARK_FAILED

            is_valid, message = baseline.validate_baseline(baseline_data)
            if not is_valid:
                print_progress(f"Baseline validation failed: {message}", "error")
                return EXIT_BENCHMARK_FAILED

            # Extract build_metrics and query_metrics from baseline
            baseline_metrics = baseline_data.get('build_metrics', {})
            baseline_query_metrics = baseline_data.get('query_metrics', {})

            # Get verification status if available
            verification = result_data.get('run_data', {}).get('verification', {})
            verification_status = "passed" if verification.get('failed_models', 0) == 0 else "failed"
            if result_data.get('verification_skipped', False):
                verification_status = "skipped"

            # Compare metrics
            print_progress(f"  Analyzing metrics for pipeline {pid}...", "info")
            comparison = report.compare_metrics(
                baseline_metrics=baseline_metrics,
                current_metrics=current_metrics,
                pipeline_name=pname,
                verification_status=verification_status,
                baseline_query_metrics=baseline_query_metrics,
                current_query_metrics=current_query_metrics,
            )

            all_comparisons.append(comparison)

            # Print the formatted summary (BUILD-TIME, QUERY-TIME, TRADEOFF)
            report.print_summary(comparison, use_colors=use_colors)

            # Print per-model detail if requested
            if show_detail:
                _print_model_detail(
                    baseline_metrics, current_metrics,
                    baseline_query_metrics, current_query_metrics,
                    pname, use_colors,
                )

            # Generate individual JSON report
            success, report_path = report.generate_report(comparison)
            if success:
                print_progress(f"  Report saved: {report_path}", "info")

            # Track regressions
            if any(c.is_regression for c in comparison.comparisons):
                has_regressions = True

        except Exception as e:
            print_progress(f"Error comparing pipeline {pid}: {e}", "error")
            return EXIT_BENCHMARK_FAILED

    # Generate aggregated report when comparing multiple pipelines
    if len(all_comparisons) > 1:
        success, agg_path = report.generate_aggregated_report(all_comparisons)
        if success:
            print_progress(f"Aggregated report saved: {agg_path}", "info")

    # Final summary
    if has_regressions:
        print_progress(
            f"Comparison completed for {pipeline_name} — REGRESSIONS DETECTED",
            "warning"
        )
    else:
        print_progress(f"Comparison completed for {pipeline_name} — no regressions", "success")

    return EXIT_SUCCESS


def _print_model_detail(
    baseline_build: Dict[str, Any],
    current_build: Dict[str, Any],
    baseline_query: Dict[str, Any],
    current_query: Dict[str, Any],
    pipeline_name: str,
    use_colors: bool = True,
) -> None:
    """
    Print per-model metric detail for build and query phases.

    Shows a table comparing each model's execution_time and bytes_scanned
    between baseline and current runs.
    """
    bold = "\033[1m" if use_colors else ""
    reset = "\033[0m" if use_colors else ""
    gray = "\033[90m" if use_colors else ""
    green = "\033[32m" if use_colors else ""
    red = "\033[31m" if use_colors else ""

    def _build_model_map(metrics: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Map model_name -> metric dict from model_details list."""
        result = {}
        for m in metrics.get('model_details', []):
            result[m.get('model_name', '').lower()] = m
        return result

    # --- Build-time model detail ---
    bl_build_map = _build_model_map(baseline_build)
    cur_build_map = _build_model_map(current_build)
    all_build_models = sorted(set(list(bl_build_map.keys()) + list(cur_build_map.keys())))

    if all_build_models:
        print(f"\n{bold}BUILD-TIME MODEL DETAIL — {pipeline_name}{reset}")
        print(f"{gray}{'Model':<35} {'Base ms':<10} {'Curr ms':<10} {'Change':<12}{reset}")
        print(f"{gray}{'-'*70}{reset}")
        for model in all_build_models:
            bl = bl_build_map.get(model, {})
            cur = cur_build_map.get(model, {})
            bl_time = bl.get('total_execution_time_ms', 0)
            cur_time = cur.get('total_execution_time_ms', 0)
            if bl_time > 0:
                pct = ((cur_time - bl_time) / bl_time) * 100
            else:
                pct = 0.0
            color = red if pct > 5 else green if pct < -5 else gray
            print(f"  {model:<33} {bl_time:<10} {cur_time:<10} {color}{pct:>+7.1f}%{reset}")

    # --- Query-time model detail ---
    bl_query_map = _build_model_map(baseline_query)
    cur_query_map = _build_model_map(current_query)
    all_query_models = sorted(set(list(bl_query_map.keys()) + list(cur_query_map.keys())))

    if all_query_models:
        print(f"\n{bold}QUERY-TIME MODEL DETAIL — {pipeline_name}{reset}")
        print(f"{gray}{'Model':<35} {'Base ms':<10} {'Curr ms':<10} {'Change':<12} {'Base bytes':<12} {'Curr bytes':<12}{reset}")
        print(f"{gray}{'-'*95}{reset}")
        for model in all_query_models:
            bl = bl_query_map.get(model, {})
            cur = cur_query_map.get(model, {})
            bl_time = bl.get('total_execution_time_ms', 0)
            cur_time = cur.get('total_execution_time_ms', 0)
            bl_bytes = bl.get('total_bytes_scanned', 0)
            cur_bytes = cur.get('total_bytes_scanned', 0)
            if bl_time > 0:
                pct = ((cur_time - bl_time) / bl_time) * 100
            else:
                pct = 0.0
            color = red if pct > 5 else green if pct < -5 else gray
            print(f"  {model:<33} {bl_time:<10} {cur_time:<10} {color}{pct:>+7.1f}%{reset}  {bl_bytes:<12} {cur_bytes:<12}")

    print()  # trailing blank line


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        prog='benchmark.py',
        description='Snowflake Benchmark System - Execute and manage performance benchmarks',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run benchmark for Pipeline A
  python benchmark.py run a

  # Run all pipelines with verbose output
  python benchmark.py run all --verbose

  # Run pipeline B without output verification
  python benchmark.py run b --no-verify

  # Save current results as baseline for Pipeline C
  python benchmark.py save-baseline c

  # Overwrite existing baseline
  python benchmark.py save-baseline a --force

  # Compare Pipeline A results against baseline
  python benchmark.py compare a

  # Compare all pipelines with per-model detail
  python benchmark.py compare all --detail

  # Compare without color (for CI/log output)
  python benchmark.py compare all --no-color

Exit Codes:
  0  - Success
  1  - Benchmark execution failed
  2  - Output verification failed
  3  - Invalid arguments
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    subparsers.required = True
    
    # 'run' command
    run_parser = subparsers.add_parser(
        'run',
        help='Execute pipeline and collect metrics'
    )
    run_parser.add_argument(
        'pipeline',
        help='Pipeline to run (a, b, c, or all)',
    )
    run_parser.add_argument(
        '--no-verify',
        action='store_true',
        help='Skip output verification step'
    )
    run_parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    run_parser.set_defaults(func=run_command)
    
    # 'save-baseline' command
    save_baseline_parser = subparsers.add_parser(
        'save-baseline',
        help='Save current results as baseline'
    )
    save_baseline_parser.add_argument(
        'pipeline',
        help='Pipeline to save baseline for (a, b, c, or all)',
    )
    save_baseline_parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing baseline without confirmation'
    )
    save_baseline_parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    save_baseline_parser.set_defaults(func=save_baseline_command)
    
    # 'compare' command
    compare_parser = subparsers.add_parser(
        'compare',
        help='Compare current results against baseline'
    )
    compare_parser.add_argument(
        'pipeline',
        help='Pipeline to compare (a, b, c, or all)',
    )
    compare_parser.add_argument(
        '--detail',
        action='store_true',
        help='Show per-model metric breakdown'
    )
    compare_parser.add_argument(
        '--no-color',
        action='store_true',
        help='Disable colored output'
    )
    compare_parser.add_argument(
        '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    compare_parser.set_defaults(func=compare_command)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Configure logging based on verbose flag
    if hasattr(args, 'verbose') and args.verbose:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
    
    # Execute command
    exit_code = args.func(args)
    sys.exit(exit_code)


if __name__ == '__main__':
    main()
