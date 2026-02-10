"""
Comparison and Reporting Engine for Benchmark Results

This module analyzes benchmark results against baselines, detects performance
regressions, and generates both machine-readable JSON reports and human-readable
console summaries with color coding.
"""

import json
import logging
import sys
import yaml
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple


# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Console handler
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# ANSI Color codes for console output
class Colors:
    """ANSI color codes for terminal output."""
    GREEN = '\033[92m'      # Improvement
    RED = '\033[91m'        # Regression
    YELLOW = '\033[93m'     # Warning
    BLUE = '\033[94m'       # Info
    GRAY = '\033[90m'       # Neutral
    RESET = '\033[0m'       # Reset to default
    BOLD = '\033[1m'        # Bold


def _load_cost_config() -> Optional[Dict[str, Any]]:
    """
    Load cost estimation configuration from benchmark_config.yml.

    Returns:
        Cost config dict if enabled, None otherwise
    """
    try:
        config_path = Path(__file__).parent / 'benchmark_config.yml'
        if not config_path.exists():
            return None

        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        cost_config = config.get('benchmarking', {}).get('cost_estimation', {})
        if not cost_config.get('enabled', False):
            return None

        return cost_config
    except Exception as e:
        logger.warning(f"Failed to load cost config: {e}")
        return None


def _calculate_cost(execution_time_ms: float, cost_config: Optional[Dict[str, Any]]) -> Optional[float]:
    """
    Calculate estimated Snowflake cost in USD based on execution time.

    Applies Snowflake's 1-minute minimum billing granularity to reflect actual billing behavior.

    Formula: billable_seconds = max(60, execution_time_seconds)
             credits = (billable_seconds / 3600) * credits_per_hour
             cost = credits * cost_per_credit_usd

    Args:
        execution_time_ms: Execution time in milliseconds
        cost_config: Cost configuration dict with credits_per_hour and cost_per_credit_usd

    Returns:
        Estimated cost in USD, or None if cost estimation disabled
    """
    if not cost_config:
        return None

    try:
        execution_time_seconds = execution_time_ms / 1000.0
        # Apply Snowflake's 1-minute minimum billing granularity
        billable_seconds = max(60.0, execution_time_seconds)

        credits_per_hour = cost_config.get('credits_per_hour', 1)
        cost_per_credit = cost_config.get('cost_per_credit_usd', 2.50)

        credits_used = (billable_seconds / 3600.0) * credits_per_hour
        cost_usd = credits_used * cost_per_credit

        return round(cost_usd, 4)
    except Exception as e:
        logger.warning(f"Failed to calculate cost: {e}")
        return None


@dataclass
class MetricComparison:
    """Comparison result for a single metric."""
    metric_name: str
    metric_key: str
    baseline_value: float
    current_value: float
    change_pct: float
    is_regression: bool
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'metric_name': self.metric_name,
            'metric_key': self.metric_key,
            'baseline_value': self.baseline_value,
            'current_value': self.current_value,
            'change_pct': self.change_pct,
            'is_regression': self.is_regression,
        }


@dataclass
class PipelineComparison:
    """Comparison results for a pipeline."""
    pipeline_name: str
    timestamp: str
    baseline_metrics: Dict[str, Any]
    current_metrics: Dict[str, Any]
    comparisons: List[MetricComparison] = field(default_factory=list)
    build_comparisons: List[MetricComparison] = field(default_factory=list)
    query_comparisons: List[MetricComparison] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    build_warnings: List[str] = field(default_factory=list)
    query_warnings: List[str] = field(default_factory=list)
    tradeoff_analysis: Dict[str, Any] = field(default_factory=dict)
    has_baseline: bool = True
    verification_status: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'pipeline': self.pipeline_name,
            'timestamp': self.timestamp,
            'baseline_metrics': self.baseline_metrics,
            'current_metrics': self.current_metrics,
            'comparison': {
                comp.metric_key: comp.change_pct for comp in self.comparisons
            },
            'build_comparison': {
                comp.metric_key: comp.change_pct for comp in self.build_comparisons
            },
            'query_comparison': {
                comp.metric_key: comp.change_pct for comp in self.query_comparisons
            },
            'warnings': self.warnings,
            'build_warnings': self.build_warnings,
            'query_warnings': self.query_warnings,
            'tradeoff_analysis': self.tradeoff_analysis,
            'verification_status': self.verification_status,
        }


def _calculate_percentage_change(baseline: float, current: float) -> float:
    """
    Calculate percentage change from baseline to current.
    
    Formula: ((current - baseline) / baseline) * 100
    
    Special cases:
    - If baseline is 0, return 0 (no change)
    - If baseline is negative, calculate absolute percentage change
    
    Args:
        baseline: Baseline metric value
        current: Current metric value
        
    Returns:
        Percentage change as float, rounded to 1 decimal place
    """
    if baseline == 0:
        # If both are 0, no change; if only baseline is 0, 0% change
        return 0.0
    
    change_pct = ((current - baseline) / abs(baseline)) * 100
    return round(change_pct, 1)


def _is_regression(metric_name: str, change_pct: float) -> bool:
    """
    Determine if a metric change is a regression (worse performance).
    
    Regressions are increases in:
    - execution_time_change_pct (higher = worse)
    - bytes_scanned_change_pct (higher = worse)
    - partitions_scanned_change_pct (higher = worse)
    
    Args:
        metric_name: Name of the metric
        change_pct: Percentage change
        
    Returns:
        True if this is a regression, False if improvement or neutral
    """
    # Only flag positive changes (increases) as regressions for these metrics
    regression_metrics = {
        'execution_time_change_pct',
        'bytes_scanned_change_pct',
        'partitions_scanned_change_pct',
    }
    
    if metric_name in regression_metrics:
        return change_pct > 0
    
    return False


def _compare_metric_set(
    baseline_metrics: Dict[str, Any],
    current_metrics: Dict[str, Any],
    metric_set_name: str = "Build",
) -> Tuple[List[MetricComparison], List[str]]:
    """
    Compare a single set of metrics (build or query) and return comparisons with warnings.

    Args:
        baseline_metrics: Baseline metrics dictionary
        current_metrics: Current metrics dictionary
        metric_set_name: Name of metric set for logging (e.g., "Build" or "Query")

    Returns:
        Tuple of (comparisons list, warnings list)
    """
    comparisons: List[MetricComparison] = []
    warnings: List[str] = []

    # Load cost config
    cost_config = _load_cost_config()

    # Extract metric values, with sensible defaults
    baseline_exec_time = baseline_metrics.get('total_execution_time_ms', 0)
    current_exec_time = current_metrics.get('total_execution_time_ms', 0)

    baseline_bytes = baseline_metrics.get('total_bytes_scanned', 0)
    current_bytes = current_metrics.get('total_bytes_scanned', 0)

    baseline_rows = baseline_metrics.get('total_rows_produced', 0)
    current_rows = current_metrics.get('total_rows_produced', 0)

    # Calculate costs if enabled
    baseline_cost = _calculate_cost(baseline_exec_time, cost_config)
    current_cost = _calculate_cost(current_exec_time, cost_config)
    
    # Calculate execution time change
    exec_time_change = _calculate_percentage_change(baseline_exec_time, current_exec_time)
    exec_time_regression = _is_regression('execution_time_change_pct', exec_time_change)
    
    comparisons.append(MetricComparison(
        metric_name=f'{metric_set_name} Execution Time',
        metric_key=f'{metric_set_name.lower()}_execution_time_change_pct',
        baseline_value=baseline_exec_time,
        current_value=current_exec_time,
        change_pct=exec_time_change,
        is_regression=exec_time_regression,
    ))
    
    if exec_time_regression:
        warnings.append(f"execution_time increased by {exec_time_change}%")
    
    # Calculate bytes scanned change
    bytes_change = _calculate_percentage_change(baseline_bytes, current_bytes)
    bytes_regression = _is_regression('bytes_scanned_change_pct', bytes_change)
    
    comparisons.append(MetricComparison(
        metric_name=f'{metric_set_name} Bytes Scanned',
        metric_key=f'{metric_set_name.lower()}_bytes_scanned_change_pct',
        baseline_value=baseline_bytes,
        current_value=current_bytes,
        change_pct=bytes_change,
        is_regression=bytes_regression,
    ))
    
    if bytes_regression:
        warnings.append(f"bytes_scanned increased by {bytes_change}%")
    
    # Calculate rows produced change
    rows_change = _calculate_percentage_change(baseline_rows, current_rows)
    rows_regression = _is_regression('rows_produced_change_pct', rows_change)

    comparisons.append(MetricComparison(
        metric_name=f'{metric_set_name} Rows Produced',
        metric_key=f'{metric_set_name.lower()}_rows_produced_change_pct',
        baseline_value=baseline_rows,
        current_value=current_rows,
        change_pct=rows_change,
        is_regression=rows_regression,
    ))

    # Add cost comparison if enabled
    if baseline_cost is not None and current_cost is not None:
        cost_change = _calculate_percentage_change(baseline_cost, current_cost)
        cost_regression = _is_regression('execution_time_change_pct', cost_change)  # Cost follows execution time

        comparisons.append(MetricComparison(
            metric_name=f'{metric_set_name} Estimated Cost',
            metric_key=f'{metric_set_name.lower()}_cost_change_pct',
            baseline_value=baseline_cost,
            current_value=current_cost,
            change_pct=cost_change,
            is_regression=cost_regression,
        ))

        if cost_regression:
            warnings.append(f"estimated_cost increased by {cost_change}% (${baseline_cost:.4f} → ${current_cost:.4f})")

    logger.debug(
        f"{metric_set_name} metrics: "
        f"exec_time={exec_time_change}%, bytes={bytes_change}%, rows={rows_change}%"
        + (f", cost={cost_change if baseline_cost else 'N/A'}%" if baseline_cost else "")
    )

    return comparisons, warnings


def _calculate_tradeoff_analysis(
    build_comparisons: List[MetricComparison],
    query_comparisons: List[MetricComparison],
) -> Dict[str, Any]:
    """
    Calculate build vs query tradeoff analysis.
    
    Analyzes the tradeoff between build-time cost and query-time performance.
    For example: views are cheap to build but expensive to query, while tables
    are expensive to build but cheap to query.
    
    Args:
        build_comparisons: Build-time metric comparisons
        query_comparisons: Query-time metric comparisons
        
    Returns:
        Dictionary with tradeoff analysis insights
    """
    tradeoff = {
        'build_vs_query_insight': '',
        'build_cost_change': 0.0,
        'query_performance_change': 0.0,
        'recommendation': '',
    }
    
    # Find execution time changes
    build_exec_change = 0.0
    query_exec_change = 0.0
    
    for comp in build_comparisons:
        if 'execution_time' in comp.metric_key:
            build_exec_change = comp.change_pct
    
    for comp in query_comparisons:
        if 'execution_time' in comp.metric_key:
            query_exec_change = comp.change_pct
    
    tradeoff['build_cost_change'] = build_exec_change
    tradeoff['query_performance_change'] = query_exec_change
    
    # Determine the tradeoff pattern
    if build_exec_change > 0 and query_exec_change < 0:
        tradeoff['build_vs_query_insight'] = (
            f"Build cost increased {build_exec_change:+.1f}%, "
            f"but query performance improved {query_exec_change:.1f}% (faster queries)"
        )
        tradeoff['recommendation'] = "FAVORABLE: Higher build cost for faster queries"
    elif build_exec_change < 0 and query_exec_change > 0:
        tradeoff['build_vs_query_insight'] = (
            f"Build cost decreased {build_exec_change:.1f}% (faster builds), "
            f"but query performance regressed {query_exec_change:+.1f}% (slower queries)"
        )
        tradeoff['recommendation'] = "UNFAVORABLE: Lower build cost at cost of slower queries"
    elif build_exec_change > 0 and query_exec_change > 0:
        tradeoff['build_vs_query_insight'] = (
            f"Both build {build_exec_change:+.1f}% and queries {query_exec_change:+.1f}% got slower"
        )
        tradeoff['recommendation'] = "REGRESSION: Both build and query performance degraded"
    elif build_exec_change < 0 and query_exec_change < 0:
        tradeoff['build_vs_query_insight'] = (
            f"Both build {build_exec_change:.1f}% and queries {query_exec_change:.1f}% got faster"
        )
        tradeoff['recommendation'] = "IMPROVEMENT: Both build and query performance improved"
    else:
        tradeoff['build_vs_query_insight'] = "Build and query metrics show minimal change"
        tradeoff['recommendation'] = "NEUTRAL: No significant performance changes"
    
    return tradeoff


def compare_metrics(
    baseline_metrics: Dict[str, Any],
    current_metrics: Dict[str, Any],
    pipeline_name: str = "",
    verification_status: str = "",
    baseline_query_metrics: Optional[Dict[str, Any]] = None,
    current_query_metrics: Optional[Dict[str, Any]] = None,
) -> PipelineComparison:
    """
    Compare current metrics against baseline metrics.
    
    Handles both build-time metrics (default) and query-time metrics when provided.
    Calculates percentage change for:
    - total_execution_time_ms
    - total_bytes_scanned
    - total_rows_produced
    
    Detects regressions for metrics that increased (worse performance).
    
    Args:
        baseline_metrics: Baseline build-time metrics dictionary
        current_metrics: Current build-time metrics dictionary
        pipeline_name: Name of the pipeline (for reporting)
        verification_status: Status from verification (pass/fail/no_baseline)
        baseline_query_metrics: Baseline query-time metrics dictionary (optional)
        current_query_metrics: Current query-time metrics dictionary (optional)
        
    Returns:
        PipelineComparison with detailed comparison results for both metric types
    """
    logger.debug(f"Comparing metrics for pipeline: {pipeline_name}")
    
    timestamp = datetime.now(timezone.utc).isoformat()
    
    # Compare build-time metrics
    build_comparisons, build_warnings = _compare_metric_set(
        baseline_metrics,
        current_metrics,
        metric_set_name="Build"
    )
    
    # Compare query-time metrics if provided
    query_comparisons: List[MetricComparison] = []
    query_warnings: List[str] = []
    
    if baseline_query_metrics is not None and current_query_metrics is not None:
        query_comparisons, query_warnings = _compare_metric_set(
            baseline_query_metrics,
            current_query_metrics,
            metric_set_name="Query"
        )
    
    # Combine comparisons for backwards compatibility
    all_comparisons = build_comparisons + query_comparisons
    all_warnings = build_warnings + query_warnings
    
    # Calculate tradeoff analysis if both metric types are present
    tradeoff_analysis: Dict[str, Any] = {}
    if query_comparisons:
        tradeoff_analysis = _calculate_tradeoff_analysis(build_comparisons, query_comparisons)
    
    logger.info(
        f"Comparison for {pipeline_name}: "
        f"build_metrics={len(build_comparisons)}, query_metrics={len(query_comparisons)}"
    )
    
    return PipelineComparison(
        pipeline_name=pipeline_name,
        timestamp=timestamp,
        baseline_metrics=baseline_metrics,
        current_metrics=current_metrics,
        comparisons=all_comparisons,
        build_comparisons=build_comparisons,
        query_comparisons=query_comparisons,
        warnings=all_warnings,
        build_warnings=build_warnings,
        query_warnings=query_warnings,
        tradeoff_analysis=tradeoff_analysis,
        has_baseline=True,
        verification_status=verification_status,
    )


def generate_report(
    comparison: PipelineComparison,
    snowflake_dir: Optional[Path] = None,
) -> Tuple[bool, str]:
    """
    Generate a JSON report file for comparison results.
    
    Creates a report file at:
    snowflake/benchmark/results/<timestamp>_<pipeline>_report.json
    
    Args:
        comparison: PipelineComparison object with results
        snowflake_dir: Path to snowflake directory (auto-detected if None)
        
    Returns:
        Tuple of (success: bool, file_path: str)
    """
    if snowflake_dir is None:
        # Find snowflake directory
        current_dir = Path.cwd()
        if current_dir.name == 'benchmark':
            snowflake_dir = current_dir.parent
        else:
            snowflake_dir = current_dir
    
    # Ensure results directory exists
    results_dir = snowflake_dir / 'benchmark' / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Create filename with timestamp and pipeline name
    # Format: <timestamp>_<pipeline>_report.json
    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    pipeline_name_normalized = comparison.pipeline_name.lower().replace(' ', '_').replace('/', '_')
    filename = f"{timestamp_str}_{pipeline_name_normalized}_report.json"
    file_path = results_dir / filename
    
    try:
        # Convert comparison to dictionary for JSON serialization
        report_data = comparison.to_dict()
        
        # Write JSON report with proper formatting
        with open(file_path, 'w') as f:
            json.dump(report_data, f, indent=2, default=str)
        
        logger.info(f"Generated report: {file_path}")
        return True, str(file_path)
    
    except Exception as e:
        logger.error(f"Failed to generate report: {e}", exc_info=True)
        return False, ""


def _print_metric_section(
    comparisons: List[MetricComparison],
    warnings: List[str],
    section_title: str,
    use_colors: bool = True,
) -> None:
    """
    Print a single metric comparison section (build or query).
    
    Args:
        comparisons: List of metric comparisons
        warnings: List of warnings for this section
        section_title: Title of the section (e.g., "BUILD-TIME METRICS" or "QUERY-TIME METRICS")
        use_colors: Whether to use ANSI color codes
    """
    blue_bold = Colors.BOLD + Colors.BLUE if use_colors else ""
    reset = Colors.RESET if use_colors else ""
    gray = Colors.GRAY if use_colors else ""
    
    if use_colors:
        print(f"{blue_bold}{section_title}:{reset}")
    else:
        print(f"\n{section_title}:")
    
    if not comparisons:
        print(f"  {gray}(No metrics available){reset}" if use_colors else "  (No metrics available)")
        return
    
    # Print header
    if use_colors:
        print(
            f"{gray}"
            f"{'Metric':<30} {'Baseline':<15} {'Current':<15} {'Change':<12}"
            f"{reset}"
        )
        print(f"{gray}{'-'*75}{reset}")
    else:
        print(f"{'Metric':<30} {'Baseline':<15} {'Current':<15} {'Change':<12}")
        print(f"{'-'*75}")
    
    # Print comparisons
    for comp in comparisons:
        # Check if this is a cost metric (values are small decimals < 10)
        is_cost_metric = 'Cost' in comp.metric_name and comp.baseline_value < 10

        if use_colors:
            if comp.is_regression:
                color = Colors.RED
                indicator = "↑ REGRESSION"
            elif comp.change_pct < 0:
                color = Colors.GREEN
                indicator = "✓ IMPROVED"
            else:
                color = Colors.GRAY
                indicator = "→ NEUTRAL"

            # Format values based on metric type
            if is_cost_metric:
                baseline_str = f"${comp.baseline_value:.4f}"
                current_str = f"${comp.current_value:.4f}"
            else:
                baseline_str = f"{comp.baseline_value:.0f}"
                current_str = f"{comp.current_value:.0f}"

            print(
                f"{comp.metric_name:<30} "
                f"{baseline_str:<15} "
                f"{current_str:<15} "
                f"{color}{comp.change_pct:>+6.1f}% {indicator}{reset}"
            )
        else:
            status = "↑ REGRESSION" if comp.is_regression else "✓ IMPROVED" if comp.change_pct < 0 else "→ NEUTRAL"

            # Format values based on metric type
            if is_cost_metric:
                baseline_str = f"${comp.baseline_value:.4f}"
                current_str = f"${comp.current_value:.4f}"
            else:
                baseline_str = f"{comp.baseline_value:.0f}"
                current_str = f"{comp.current_value:.0f}"

            print(
                f"{comp.metric_name:<30} "
                f"{baseline_str:<15} "
                f"{current_str:<15} "
                f"{comp.change_pct:>+6.1f}% {status}"
            )
    
    # Print warnings for this section
    if warnings:
        if use_colors:
            print(f"{Colors.YELLOW}Warnings:{reset}")
            for warning in warnings:
                print(f"  {Colors.YELLOW}⚠{reset} {warning}")
        else:
            print("Warnings:")
            for warning in warnings:
                print(f"  ⚠ {warning}")


def print_summary(
    comparison: PipelineComparison,
    use_colors: bool = True,
) -> None:
    """
    Print a human-readable summary of comparison results with color coding.
    
    For pipelines with both build and query metrics, displays:
    - Separate BUILD-TIME METRICS section (DDL operations)
    - Separate QUERY-TIME METRICS section (actual query execution)
    - BUILD vs QUERY TRADEOFF analysis (cost vs performance insights)
    
    Color scheme:
    - Green: Improvements (negative change in execution_time, bytes_scanned)
    - Red: Regressions (positive change in execution_time, bytes_scanned)
    - Yellow: Warnings for any regressions
    - Blue: Pipeline name and headers
    
    Args:
        comparison: PipelineComparison object with results
        use_colors: Whether to use ANSI color codes (default True)
    """
    blue_bold = Colors.BOLD + Colors.BLUE if use_colors else ""
    reset = Colors.RESET if use_colors else ""
    
    # Print header
    if use_colors:
        print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*75}{reset}")
        print(f"{blue_bold}Pipeline:{reset} {comparison.pipeline_name}")
        print(f"{blue_bold}Timestamp:{reset} {comparison.timestamp}")
        print(f"{Colors.BOLD}{Colors.BLUE}{'='*75}{reset}")
    else:
        print(f"\n{'='*75}")
        print(f"Pipeline: {comparison.pipeline_name}")
        print(f"Timestamp: {comparison.timestamp}")
        print(f"{'='*75}")
    
    if not comparison.has_baseline:
        msg = "No baseline available for comparison (first run)"
        if use_colors:
            print(f"{Colors.YELLOW}{msg}{reset}")
        else:
            print(msg)
        return
    
    # Check if we have both build and query metrics
    has_build = bool(comparison.build_comparisons)
    has_query = bool(comparison.query_comparisons)
    
    if has_build and has_query:
        # Print dual-section output
        print()  # Blank line
        _print_metric_section(
            comparison.build_comparisons,
            comparison.build_warnings,
            "BUILD-TIME METRICS",
            use_colors
        )
        
        print()  # Blank line
        _print_metric_section(
            comparison.query_comparisons,
            comparison.query_warnings,
            "QUERY-TIME METRICS",
            use_colors
        )
        
        # Print tradeoff analysis
        if comparison.tradeoff_analysis:
            print()  # Blank line
            if use_colors:
                print(f"{blue_bold}BUILD vs QUERY TRADEOFF ANALYSIS:{reset}")
                print(f"{Colors.BLUE}{'-'*75}{reset}")
                print(f"{Colors.BOLD}{comparison.tradeoff_analysis.get('build_vs_query_insight', 'N/A')}{reset}")
                
                recommendation = comparison.tradeoff_analysis.get('recommendation', '')
                if 'FAVORABLE' in recommendation:
                    rec_color = Colors.GREEN
                elif 'UNFAVORABLE' in recommendation:
                    rec_color = Colors.RED
                elif 'REGRESSION' in recommendation:
                    rec_color = Colors.RED
                elif 'IMPROVEMENT' in recommendation:
                    rec_color = Colors.GREEN
                else:
                    rec_color = Colors.GRAY
                
                print(f"{rec_color}→ {recommendation}{reset}")
            else:
                print("\nBUILD vs QUERY TRADEOFF ANALYSIS:")
                print(f"{'-'*75}")
                print(comparison.tradeoff_analysis.get('build_vs_query_insight', 'N/A'))
                print(f"→ {comparison.tradeoff_analysis.get('recommendation', 'N/A')}")
    else:
        # Fall back to simple format for backward compatibility or single metric type
        section_title = "QUERY-TIME METRICS" if has_query else "BUILD-TIME METRICS" if has_build else "METRIC COMPARISONS"
        comparisons = comparison.query_comparisons if has_query else comparison.build_comparisons if has_build else comparison.comparisons
        warnings = comparison.query_warnings if has_query else comparison.build_warnings if has_build else comparison.warnings
        
        print()  # Blank line
        _print_metric_section(comparisons, warnings, section_title, use_colors)
    
    # Print verification status
    if comparison.verification_status:
        if use_colors:
            status_color = Colors.GREEN if comparison.verification_status.lower() == 'pass' else Colors.RED
            print(f"\n{blue_bold}Verification Status:{reset} {status_color}{comparison.verification_status.upper()}{reset}")
        else:
            print(f"\nVerification Status: {comparison.verification_status.upper()}")
    
    if use_colors:
        print(f"{Colors.BOLD}{Colors.BLUE}{'='*75}{reset}\n")
    else:
        print(f"{'='*75}\n")


def generate_aggregated_report(
    pipeline_comparisons: List[PipelineComparison],
    snowflake_dir: Optional[Path] = None,
) -> Tuple[bool, str]:
    """
    Generate an aggregated JSON report for multiple pipelines.
    
    Creates an "all pipelines" report combining results from multiple comparisons.
    
    Args:
        pipeline_comparisons: List of PipelineComparison objects
        snowflake_dir: Path to snowflake directory (auto-detected if None)
        
    Returns:
        Tuple of (success: bool, file_path: str)
    """
    if snowflake_dir is None:
        # Find snowflake directory
        current_dir = Path.cwd()
        if current_dir.name == 'benchmark':
            snowflake_dir = current_dir.parent
        else:
            snowflake_dir = current_dir
    
    # Ensure results directory exists
    results_dir = snowflake_dir / 'benchmark' / 'results'
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # Create filename for aggregated report
    timestamp_str = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    filename = f"{timestamp_str}_all_pipelines_report.json"
    file_path = results_dir / filename
    
    try:
        # Build aggregated report
        reports = [comp.to_dict() for comp in pipeline_comparisons]
        
        aggregated_data = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'pipeline_count': len(pipeline_comparisons),
            'pipelines': reports,
            'summary': {
                'total_regressions': sum(
                    len(comp.warnings) for comp in pipeline_comparisons
                ),
                'total_pipelines_with_baseline': sum(
                    1 for comp in pipeline_comparisons if comp.has_baseline
                ),
            },
        }
        
        # Write JSON report
        with open(file_path, 'w') as f:
            json.dump(aggregated_data, f, indent=2, default=str)
        
        logger.info(f"Generated aggregated report: {file_path}")
        return True, str(file_path)
    
    except Exception as e:
        logger.error(f"Failed to generate aggregated report: {e}", exc_info=True)
        return False, ""


def print_aggregated_summary(
    pipeline_comparisons: List[PipelineComparison],
    use_colors: bool = True,
) -> None:
    """
    Print an aggregated summary for all pipelines.
    
    Args:
        pipeline_comparisons: List of PipelineComparison objects
        use_colors: Whether to use ANSI color codes (default True)
    """
    if not use_colors:
        print(f"\n{'='*70}")
        print("AGGREGATED BENCHMARK REPORT - ALL PIPELINES")
        print(f"{'='*70}")
        print(f"\nTotal Pipelines: {len(pipeline_comparisons)}")
        print(f"Pipelines with Baseline: {sum(1 for c in pipeline_comparisons if c.has_baseline)}")
        print(f"Total Warnings/Regressions: {sum(len(c.warnings) for c in pipeline_comparisons)}")
        
        print(f"\n{'Pipeline':<20} {'Status':<12} {'Regressions':<15}")
        print(f"{'-'*70}")
        
        for comp in pipeline_comparisons:
            status = "OK" if not comp.warnings else "WARNINGS"
            print(
                f"{comp.pipeline_name:<20} "
                f"{status:<12} "
                f"{len(comp.warnings):<15}"
            )
        
        print(f"{'='*70}\n")
    
    else:
        blue_bold = Colors.BOLD + Colors.BLUE
        reset = Colors.RESET
        
        print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*70}{reset}")
        print(f"{blue_bold}AGGREGATED BENCHMARK REPORT - ALL PIPELINES{reset}")
        print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{reset}")
        
        print(f"\n{blue_bold}Total Pipelines:{reset} {len(pipeline_comparisons)}")
        print(
            f"{blue_bold}Pipelines with Baseline:{reset} "
            f"{sum(1 for c in pipeline_comparisons if c.has_baseline)}"
        )
        
        total_regressions = sum(len(c.warnings) for c in pipeline_comparisons)
        if total_regressions > 0:
            print(
                f"{blue_bold}Total Warnings/Regressions:{reset} "
                f"{Colors.RED}{total_regressions}{reset}"
            )
        else:
            print(
                f"{blue_bold}Total Warnings/Regressions:{reset} "
                f"{Colors.GREEN}0{reset}"
            )
        
        print(
            f"\n{Colors.GRAY}"
            f"{'Pipeline':<20} {'Status':<12} {'Regressions':<15}"
            f"{reset}"
        )
        print(f"{Colors.GRAY}{'-'*70}{reset}")
        
        for comp in pipeline_comparisons:
            if comp.warnings:
                status_color = Colors.RED
                status = "⚠ WARNINGS"
            else:
                status_color = Colors.GREEN
                status = "✓ OK"
            
            print(
                f"{comp.pipeline_name:<20} "
                f"{status_color}{status:<12}{reset} "
                f"{len(comp.warnings):<15}"
            )
        
        print(f"{Colors.BOLD}{Colors.BLUE}{'='*70}{reset}\n")


if __name__ == '__main__':
    # Example usage
    logger.info("Running report generator example")
    
    try:
        # Example baseline metrics
        example_baseline = {
            'pipeline_id': 'A',
            'timestamp': '2024-01-15T10:00:00Z',
            'total_execution_time_ms': 5000,
            'total_bytes_scanned': 1000000,
            'total_rows_produced': 10000,
        }
        
        # Example current metrics
        example_current = {
            'pipeline_id': 'A',
            'timestamp': '2024-01-15T10:30:00Z',
            'total_execution_time_ms': 5750,  # 15% increase - regression
            'total_bytes_scanned': 950000,    # 5% decrease - improvement
            'total_rows_produced': 10000,     # No change
        }
        
        # Compare metrics
        comparison = compare_metrics(
            baseline_metrics=example_baseline,
            current_metrics=example_current,
            pipeline_name='Pipeline A',
            verification_status='pass',
        )
        
        logger.info(f"Comparison result: {comparison}")
        
        # Print summary
        print_summary(comparison, use_colors=True)
        
        # Generate report
        success, file_path = generate_report(comparison)
        logger.info(f"Report generated: {success} at {file_path}")
    
    except Exception as e:
        logger.error(f"Error in example: {e}", exc_info=True)
        sys.exit(1)
