#!/usr/bin/env python3
"""
Benchmark validation report comparator.

Loads and compares two validation reports (baseline from Postgres, candidate from Snowflake),
identifies differences in row counts and data hashes, and produces both human-readable console
output and a detailed JSON diff file.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Try to import colorama for colored output, fallback to plain text
try:
    from colorama import Fore, Style, init
    HAS_COLOR = True
    init(autoreset=True)
except ImportError:
    HAS_COLOR = False


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Hardcoded list of 10 marts models (must match generate-report.py)
MARTS_MODELS = [
    'fact_portfolio_summary',
    'report_portfolio_overview',
    'fact_portfolio_pnl',
    'fact_trade_activity',
    'report_daily_pnl',
    'fact_fund_performance',
    'fact_cashflow_waterfall',
    'fact_portfolio_attribution',
    'report_ic_dashboard',
    'report_lp_quarterly'
]


class ColoredText:
    """Helper class for colored output."""

    @staticmethod
    def green(text: str) -> str:
        """Return green colored text if colors are available."""
        if HAS_COLOR:
            return f"{Fore.GREEN}{text}{Style.RESET_ALL}"
        return text

    @staticmethod
    def red(text: str) -> str:
        """Return red colored text if colors are available."""
        if HAS_COLOR:
            return f"{Fore.RED}{text}{Style.RESET_ALL}"
        return text

    @staticmethod
    def yellow(text: str) -> str:
        """Return yellow colored text if colors are available."""
        if HAS_COLOR:
            return f"{Fore.YELLOW}{text}{Style.RESET_ALL}"
        return text


class ReportLoader:
    """Loads and validates validation reports."""

    @staticmethod
    def load_report(file_path: str) -> Dict[str, Any]:
        """
        Load a validation report from JSON file.

        Args:
            file_path: Path to the report.json file

        Returns:
            Dictionary containing the report data

        Raises:
            FileNotFoundError: If file doesn't exist
            json.JSONDecodeError: If JSON is malformed
            ValueError: If report structure is invalid
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Report file not found: {file_path}")

        try:
            with open(file_path, 'r') as f:
                report = json.load(f)
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Failed to parse JSON from {file_path}: {e.msg}",
                e.doc,
                e.pos
            )

        # Validate structure
        ReportLoader._validate_report_structure(report, file_path)

        return report

    @staticmethod
    def _validate_report_structure(report: Dict[str, Any], file_path: str) -> None:
        """
        Validate that report has expected structure.

        Args:
            report: Report dictionary to validate
            file_path: Path to report file (for error messages)

        Raises:
            ValueError: If report structure is invalid
        """
        required_keys = {'generated_at', 'dialect', 'database', 'schema', 'models'}
        if not isinstance(report, dict):
            raise ValueError(f"Report must be a JSON object in {file_path}")

        missing_keys = required_keys - set(report.keys())
        if missing_keys:
            raise ValueError(
                f"Report in {file_path} missing required keys: {missing_keys}"
            )

        if not isinstance(report['models'], dict):
            raise ValueError(
                f"Report 'models' must be a JSON object in {file_path}"
            )


class ReportComparator:
    """Compares two validation reports."""

    def __init__(self, baseline_report: Dict[str, Any], candidate_report: Dict[str, Any]):
        """
        Initialize the comparator.

        Args:
            baseline_report: Baseline report dictionary
            candidate_report: Candidate report dictionary
        """
        self.baseline = baseline_report
        self.candidate = candidate_report
        self.results = {
            'passed': [],
            'failed': [],
            'missing': [],
            'extra': []
        }

    def compare(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Compare all models between baseline and candidate.

        Returns:
            Dictionary with comparison results
        """
        baseline_models = set(self.baseline['models'].keys())
        candidate_models = set(self.candidate['models'].keys())

        # Compare each model in baseline
        for model_name in MARTS_MODELS:
            if model_name not in baseline_models:
                # Model not in baseline, skip
                continue

            if model_name not in candidate_models:
                # Model exists in baseline but not in candidate
                self.results['missing'].append({
                    'model': model_name,
                    'reason': 'Not found in candidate report'
                })
            else:
                # Compare the model
                baseline_model = self.baseline['models'][model_name]
                candidate_model = self.candidate['models'][model_name]

                comparison = self._compare_model(
                    model_name,
                    baseline_model,
                    candidate_model
                )
                if comparison['passed']:
                    self.results['passed'].append(comparison)
                else:
                    self.results['failed'].append(comparison)

        # Check for extra models in candidate
        for model_name in candidate_models:
            if model_name not in baseline_models and model_name in MARTS_MODELS:
                self.results['extra'].append({
                    'model': model_name,
                    'reason': 'Not found in baseline report'
                })

        return self.results

    def _compare_model(
        self,
        model_name: str,
        baseline_model: Dict[str, Any],
        candidate_model: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Compare a single model between baseline and candidate.

        Args:
            model_name: Name of the model
            baseline_model: Model data from baseline
            candidate_model: Model data from candidate

        Returns:
            Dictionary with comparison result
        """
        issues = []

        # Handle errors in either report
        if 'error' in baseline_model and baseline_model['error']:
            issues.append('Baseline report has error')
        if 'error' in candidate_model and candidate_model['error']:
            issues.append('Candidate report has error')

        # Compare row counts
        baseline_row_count = baseline_model.get('row_count')
        candidate_row_count = candidate_model.get('row_count')

        if baseline_row_count != candidate_row_count:
            issues.append(
                f'row_count mismatch: {baseline_row_count} vs {candidate_row_count}'
            )

        # Compare data hashes
        baseline_hash = baseline_model.get('data_hash')
        candidate_hash = candidate_model.get('data_hash')

        if baseline_hash != candidate_hash:
            issues.append(
                f'data_hash mismatch'
            )

        passed = len(issues) == 0

        return {
            'model': model_name,
            'passed': passed,
            'baseline_row_count': baseline_row_count,
            'candidate_row_count': candidate_row_count,
            'baseline_hash': baseline_hash,
            'candidate_hash': candidate_hash,
            'issues': issues
        }


class ResultFormatter:
    """Formats and outputs comparison results."""

    def __init__(self, results: Dict[str, List[Dict[str, Any]]], baseline_metadata: Dict[str, Any], candidate_metadata: Dict[str, Any]):
        """
        Initialize the formatter.

        Args:
            results: Comparison results from ReportComparator
            baseline_metadata: Metadata from baseline report
            candidate_metadata: Metadata from candidate report
        """
        self.results = results
        self.baseline_metadata = baseline_metadata
        self.candidate_metadata = candidate_metadata

    def print_console_output(self) -> None:
        """Print human-readable console output with symbols and colors."""
        print("\n" + "=" * 80)
        print("Validation Report Comparison")
        print("=" * 80)

        # Print metadata
        print(f"\nBaseline: {self.baseline_metadata['dialect']} ({self.baseline_metadata.get('database', 'N/A')})")
        print(f"  Generated: {self.baseline_metadata.get('generated_at', 'N/A')}")

        print(f"\nCandidate: {self.candidate_metadata['dialect']} ({self.candidate_metadata.get('database', 'N/A')})")
        print(f"  Generated: {self.candidate_metadata.get('generated_at', 'N/A')}")

        print("\n" + "-" * 80)
        print("Results:")
        print("-" * 80 + "\n")

        # Print passed models
        if self.results['passed']:
            for result in self.results['passed']:
                model = result['model']
                row_count = result['baseline_row_count']
                status = ColoredText.green(f"✓ PASS")
                print(f"{status}: {model} ({row_count} rows, hash match)")

        # Print failed models
        if self.results['failed']:
            for result in self.results['failed']:
                model = result['model']
                baseline_rc = result['baseline_row_count']
                candidate_rc = result['candidate_row_count']
                status = ColoredText.red(f"✗ FAIL")
                issues_str = ', '.join(result['issues'])
                print(f"{status}: {model}")
                print(f"        {issues_str}")

        # Print missing models
        if self.results['missing']:
            for result in self.results['missing']:
                model = result['model']
                status = ColoredText.yellow(f"⚠ MISSING")
                print(f"{status}: {model} (not found in candidate)")

        # Print extra models
        if self.results['extra']:
            for result in self.results['extra']:
                model = result['model']
                status = ColoredText.yellow(f"⚠ EXTRA")
                print(f"{status}: {model} (not found in baseline)")

        # Print summary
        print("\n" + "-" * 80)
        total = len(self.results['passed']) + len(self.results['failed']) + len(self.results['missing']) + len(self.results['extra'])
        passed = len(self.results['passed'])
        failed = len(self.results['failed'])
        missing = len(self.results['missing'])
        extra = len(self.results['extra'])

        print(f"Summary:")
        print(f"  Total models: {total}")
        print(f"  Passed: {ColoredText.green(str(passed))}")
        print(f"  Failed: {ColoredText.red(str(failed))}")
        print(f"  Missing: {ColoredText.yellow(str(missing))}")
        print(f"  Extra: {ColoredText.yellow(str(extra))}")
        print("=" * 80 + "\n")

    def generate_json_diff(self) -> Dict[str, Any]:
        """
        Generate detailed JSON diff structure.

        Returns:
            Dictionary with comparison metadata and results
        """
        return {
            'comparison_timestamp': datetime.utcnow().isoformat() + 'Z',
            'baseline_metadata': {
                'dialect': self.baseline_metadata['dialect'],
                'database': self.baseline_metadata.get('database'),
                'schema': self.baseline_metadata.get('schema'),
                'generated_at': self.baseline_metadata.get('generated_at')
            },
            'candidate_metadata': {
                'dialect': self.candidate_metadata['dialect'],
                'database': self.candidate_metadata.get('database'),
                'schema': self.candidate_metadata.get('schema'),
                'generated_at': self.candidate_metadata.get('generated_at')
            },
            'results': {
                'passed': self.results['passed'],
                'failed': self.results['failed'],
                'missing': self.results['missing'],
                'extra': self.results['extra']
            },
            'summary': {
                'total_models': len(self.results['passed']) + len(self.results['failed']) + len(self.results['missing']) + len(self.results['extra']),
                'passed': len(self.results['passed']),
                'failed': len(self.results['failed']),
                'missing': len(self.results['missing']),
                'extra': len(self.results['extra'])
            }
        }

    def save_json_diff(self, output_path: str) -> None:
        """
        Save JSON diff to file.

        Args:
            output_path: Path to save the diff JSON file
        """
        diff_data = self.generate_json_diff()

        # Create output directory if needed
        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        with open(output_path, 'w') as f:
            json.dump(diff_data, f, indent=2)

        logger.info(f"JSON diff saved to {output_path}")


def main():
    """Main entry point for the comparison script."""
    parser = argparse.ArgumentParser(
        description='Compare validation reports from baseline and candidate databases'
    )
    parser.add_argument(
        '--baseline',
        required=True,
        help='Path to baseline report.json file (Postgres)'
    )
    parser.add_argument(
        '--candidate',
        required=True,
        help='Path to candidate report.json file (Snowflake)'
    )
    parser.add_argument(
        '--output',
        default='./comparison_diff.json',
        help='Path to save the comparison diff JSON file (default: ./comparison_diff.json)'
    )

    args = parser.parse_args()

    try:
        # Load reports
        logger.info(f"Loading baseline report from {args.baseline}")
        baseline_report = ReportLoader.load_report(args.baseline)

        logger.info(f"Loading candidate report from {args.candidate}")
        candidate_report = ReportLoader.load_report(args.candidate)

        # Compare reports
        logger.info("Comparing reports...")
        comparator = ReportComparator(baseline_report, candidate_report)
        results = comparator.compare()

        # Format and output results
        formatter = ResultFormatter(
            results,
            baseline_report,
            candidate_report
        )
        formatter.print_console_output()
        formatter.save_json_diff(args.output)

        # Determine exit code
        has_failures = len(results['failed']) > 0 or len(results['missing']) > 0
        exit_code = 1 if has_failures else 0

        if has_failures:
            logger.warning(f"Comparison completed with failures (exit code {exit_code})")
        else:
            logger.info(f"Comparison completed successfully (exit code {exit_code})")

        return exit_code

    except FileNotFoundError as e:
        logger.error(f"File error: {e}")
        return 1
    except json.JSONDecodeError as e:
        logger.error(f"JSON parsing error: {e}")
        return 1
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        return 1
    except KeyboardInterrupt:
        logger.info("Comparison cancelled by user")
        return 130
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
