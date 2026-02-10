"""
Baseline Management System

This module manages benchmark baselines that store snapshots of pipeline
performance and output correctness for later comparison. Baselines are stored
as JSON files with metrics, output fingerprints, and metadata.
"""

import json
import logging
import os
import subprocess
import sys
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


@dataclass
class BaselineMetadata:
    """Metadata for a baseline snapshot."""
    timestamp: str
    git_commit: Optional[str] = None
    dbt_version: Optional[str] = None
    username: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'timestamp': self.timestamp,
            'git_commit': self.git_commit,
            'dbt_version': self.dbt_version,
            'username': self.username,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaselineMetadata':
        """Create from dictionary."""
        return cls(
            timestamp=data.get('timestamp', ''),
            git_commit=data.get('git_commit'),
            dbt_version=data.get('dbt_version'),
            username=data.get('username'),
        )


@dataclass
class BaselineData:
    """Complete baseline data including metrics and fingerprints."""
    pipeline_name: str
    build_metrics: Dict[str, Any]
    query_metrics: Dict[str, Any]
    fingerprints: List[Dict[str, Any]]
    metadata: BaselineMetadata
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'pipeline_name': self.pipeline_name,
            'build_metrics': self.build_metrics,
            'query_metrics': self.query_metrics,
            'fingerprints': self.fingerprints,
            'metadata': self.metadata.to_dict(),
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaselineData':
        """Create from dictionary."""
        metadata_dict = data.get('metadata', {})
        return cls(
            pipeline_name=data.get('pipeline_name', ''),
            build_metrics=data.get('build_metrics', {}),
            query_metrics=data.get('query_metrics', {}),
            fingerprints=data.get('fingerprints', []),
            metadata=BaselineMetadata.from_dict(metadata_dict),
        )


def _get_git_commit_hash() -> Optional[str]:
    """
    Capture git commit hash using subprocess.
    
    Returns:
        Commit hash string or None if git is not available or not in a git repo
    """
    try:
        result = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            cwd=Path.cwd(),
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            commit_hash = result.stdout.strip()
            logger.debug(f"Captured git commit hash: {commit_hash}")
            return commit_hash
        else:
            logger.debug("Git command failed or not in a git repository")
            return None
    except FileNotFoundError:
        logger.debug("Git not installed or not in PATH")
        return None
    except subprocess.TimeoutExpired:
        logger.debug("Git command timed out")
        return None
    except Exception as e:
        logger.debug(f"Error capturing git commit hash: {e}")
        return None


def _get_dbt_version() -> Optional[str]:
    """
    Capture dbt version from subprocess or package metadata.
    
    Returns:
        dbt version string or None if dbt is not available
    """
    try:
        # Try running dbt --version
        result = subprocess.run(
            ['dbt', '--version'],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            # Output is like "dbt version: 1.5.0\n..."
            output = result.stdout.strip()
            # Extract version line
            for line in output.split('\n'):
                if 'dbt version:' in line:
                    version = line.split('dbt version:')[1].strip()
                    logger.debug(f"Captured dbt version: {version}")
                    return version
            # If no version line found, return first line
            first_line = output.split('\n')[0]
            logger.debug(f"Captured dbt version (first line): {first_line}")
            return first_line
        else:
            logger.debug("dbt command failed")
            return None
    except FileNotFoundError:
        logger.debug("dbt not installed or not in PATH")
        return None
    except subprocess.TimeoutExpired:
        logger.debug("dbt command timed out")
        return None
    except Exception as e:
        logger.debug(f"Error capturing dbt version: {e}")
        return None


def _get_username() -> Optional[str]:
    """
    Get username from environment variable or system user.
    
    Returns:
        Username or None if not available
    """
    try:
        # Try environment variables first
        username = os.getenv('USER')
        if not username:
            username = os.getenv('USERNAME')
        if not username:
            # Try to get system user using subprocess (cross-platform)
            import getpass
            username = getpass.getuser()
        
        if username:
            logger.debug(f"Captured username: {username}")
            return username
        else:
            logger.debug("Could not determine username")
            return None
    except Exception as e:
        logger.debug(f"Error capturing username: {e}")
        return None


def _get_baselines_dir(snowflake_dir: Optional[Path] = None) -> Path:
    """
    Get the baselines directory path.
    
    Args:
        snowflake_dir: Path to snowflake directory (auto-detected if None)
        
    Returns:
        Path to baselines directory
    """
    if snowflake_dir is None:
        # Find snowflake directory
        current_dir = Path.cwd()
        if current_dir.name == 'benchmark':
            snowflake_dir = current_dir.parent
        else:
            snowflake_dir = current_dir
    
    baselines_dir = snowflake_dir / 'benchmark' / 'baselines'
    return baselines_dir


def _get_baseline_file_path(pipeline_name: str, snowflake_dir: Optional[Path] = None) -> Path:
    """
    Get the full path for a baseline JSON file.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., "Pipeline A")
        snowflake_dir: Path to snowflake directory (auto-detected if None)
        
    Returns:
        Path to baseline file
    """
    baselines_dir = _get_baselines_dir(snowflake_dir)
    
    # Normalize pipeline name for filename (lowercase, replace spaces with underscores)
    filename = f"{pipeline_name.lower().replace(' ', '_')}_baseline.json"
    return baselines_dir / filename


def save_baseline(
    pipeline_name: str,
    build_metrics: Dict[str, Any],
    query_metrics: Dict[str, Any],
    fingerprints: List[Dict[str, Any]],
    snowflake_dir: Optional[Path] = None,
) -> bool:
    """
    Save a baseline snapshot to JSON file.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., "Pipeline A")
        build_metrics: Build-time metrics dictionary from metrics_collector
        query_metrics: Query-time metrics dictionary from query execution
        fingerprints: List of fingerprint dictionaries from verify_output
        snowflake_dir: Path to snowflake directory (auto-detected if None)
        
    Returns:
        True if successful, False otherwise
        
    Raises:
        ValueError: If pipeline_name is empty or required metrics are missing
    """
    if not pipeline_name:
        logger.error("pipeline_name cannot be empty")
        raise ValueError("pipeline_name cannot be empty")
    
    if not isinstance(build_metrics, dict):
        logger.error("build_metrics must be a dictionary")
        raise ValueError("build_metrics must be a dictionary")
    
    if not isinstance(query_metrics, dict):
        logger.error("query_metrics must be a dictionary")
        raise ValueError("query_metrics must be a dictionary")
    
    logger.info(f"Saving baseline for pipeline: {pipeline_name}")
    logger.debug(f"  build_metrics keys: {list(build_metrics.keys())}")
    logger.debug(f"  query_metrics keys: {list(query_metrics.keys())}")
    
    try:
        # Ensure baselines directory exists
        baselines_dir = _get_baselines_dir(snowflake_dir)
        baselines_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Ensured baselines directory exists: {baselines_dir}")
        
        # Create metadata
        metadata = BaselineMetadata(
            timestamp=datetime.now(timezone.utc).isoformat(),
            git_commit=_get_git_commit_hash(),
            dbt_version=_get_dbt_version(),
            username=_get_username(),
        )
        
        # Create baseline data with both metrics sections
        baseline_data = BaselineData(
            pipeline_name=pipeline_name,
            build_metrics=build_metrics,
            query_metrics=query_metrics,
            fingerprints=fingerprints,
            metadata=metadata,
        )
        
        # Get file path
        file_path = _get_baseline_file_path(pipeline_name, snowflake_dir)
        
        # Write to JSON file with proper formatting
        with open(file_path, 'w') as f:
            json.dump(baseline_data.to_dict(), f, indent=2, default=str)
        
        logger.info(f"Successfully saved baseline to {file_path}")
        logger.info(f"  Baseline contains build_metrics and query_metrics sections")
        return True
    
    except Exception as e:
        logger.error(f"Failed to save baseline: {e}", exc_info=True)
        return False


def load_baseline(
    pipeline_name: str,
    snowflake_dir: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    """
    Load a baseline snapshot from JSON file.
    
    Supports both new format (with build_metrics and query_metrics) and 
    detects old format for helpful error message.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., "Pipeline A")
        snowflake_dir: Path to snowflake directory (auto-detected if None)
        
    Returns:
        Dictionary with baseline data or None if not found or error
        
    Raises:
        ValueError: If baseline uses old format (contains 'metrics' but not 'build_metrics')
    """
    logger.debug(f"Loading baseline for pipeline: {pipeline_name}")
    
    try:
        # Get file path
        file_path = _get_baseline_file_path(pipeline_name, snowflake_dir)
        
        # Check if file exists
        if not file_path.exists():
            logger.debug(f"Baseline file not found: {file_path}")
            return None
        
        # Read JSON file
        with open(file_path, 'r') as f:
            data = json.load(f)
        
        # Detect old format and provide helpful error
        if 'metrics' in data and 'build_metrics' not in data:
            error_msg = (
                "Baseline file uses old format. "
                "Please regenerate baseline with updated benchmark.py"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Successfully loaded baseline from {file_path}")
        return data
    
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse baseline JSON: {e}")
        return None
    except ValueError as e:
        # Re-raise ValueError for old format detection
        logger.error(f"Baseline format error: {e}")
        raise
    except Exception as e:
        logger.error(f"Failed to load baseline: {e}")
        return None


def validate_baseline(
    baseline_data: Optional[Dict[str, Any]],
) -> Tuple[bool, str]:
    """
    Validate baseline data for required fields.
    
    Checks for new format with both build_metrics and query_metrics.
    
    Args:
        baseline_data: Baseline data dictionary (from load_baseline)
        
    Returns:
        Tuple of (is_valid: bool, message: str)
    """
    if baseline_data is None:
        return False, "Baseline data is None"
    
    if not isinstance(baseline_data, dict):
        return False, "Baseline data is not a dictionary"
    
    # Check for old format
    if 'metrics' in baseline_data and 'build_metrics' not in baseline_data:
        return False, (
            "Baseline file uses old format. "
            "Please regenerate baseline with updated benchmark.py"
        )
    
    # Check required fields for new format
    required_fields = ['pipeline_name', 'build_metrics', 'query_metrics', 'fingerprints', 'metadata']
    missing_fields = [field for field in required_fields if field not in baseline_data]
    
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    
    # Check metadata fields
    metadata = baseline_data.get('metadata', {})
    if not isinstance(metadata, dict):
        return False, "Metadata is not a dictionary"
    
    if 'timestamp' not in metadata:
        return False, "Missing timestamp in metadata"
    
    # Validate fingerprints structure
    fingerprints = baseline_data.get('fingerprints', [])
    if not isinstance(fingerprints, list):
        return False, "Fingerprints is not a list"
    
    # Validate build_metrics structure
    build_metrics = baseline_data.get('build_metrics', {})
    if not isinstance(build_metrics, dict):
        return False, "build_metrics is not a dictionary"
    
    # Validate query_metrics structure
    query_metrics = baseline_data.get('query_metrics', {})
    if not isinstance(query_metrics, dict):
        return False, "query_metrics is not a dictionary"
    
    return True, "Baseline data is valid"


def clear_baseline(
    pipeline_name: str,
    snowflake_dir: Optional[Path] = None,
) -> bool:
    """
    Delete baseline file for specified pipeline.
    
    Args:
        pipeline_name: Name of the pipeline (e.g., "Pipeline A")
        snowflake_dir: Path to snowflake directory (auto-detected if None)
        
    Returns:
        True if deleted or file didn't exist, False if error
    """
    logger.info(f"Clearing baseline for pipeline: {pipeline_name}")
    
    try:
        # Get file path
        file_path = _get_baseline_file_path(pipeline_name, snowflake_dir)
        
        # Check if file exists
        if not file_path.exists():
            logger.debug(f"Baseline file not found, nothing to delete: {file_path}")
            return True
        
        # Delete file
        file_path.unlink()
        logger.info(f"Successfully deleted baseline file: {file_path}")
        return True
    
    except Exception as e:
        logger.error(f"Failed to delete baseline: {e}")
        return False


if __name__ == '__main__':
    # Example usage
    logger.info("Running baseline management example")
    
    try:
        # Example build metrics data (from build-time collection)
        example_build_metrics = {
            'pipeline_id': 'A',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'total_execution_time_ms': 5000,
            'total_bytes_scanned': 1000000,
            'total_rows_produced': 10000,
            'model_metrics': [
                {
                    'model_name': 'FACT_PORTFOLIO_SUMMARY',
                    'fqn': 'DBT_DEMO.DEV.FACT_PORTFOLIO_SUMMARY',
                    'total_execution_time_ms': 2000,
                    'total_bytes_scanned': 500000,
                    'total_rows_produced': 5000,
                    'query_count': 1,
                }
            ],
        }
        
        # Example query metrics data (from query-time execution)
        example_query_metrics = {
            'pipeline_id': 'A',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'total_execution_time_ms': 1000,
            'total_bytes_scanned': 100000,
            'total_rows_produced': 5000,
            'model_metrics': [
                {
                    'model_name': 'FACT_PORTFOLIO_SUMMARY',
                    'fqn': 'DBT_DEMO.DEV.FACT_PORTFOLIO_SUMMARY',
                    'total_execution_time_ms': 1000,
                    'total_bytes_scanned': 100000,
                    'total_rows_produced': 5000,
                    'query_count': 1,
                }
            ],
        }
        
        # Example fingerprints data
        example_fingerprints = [
            {
                'model_name': 'FACT_PORTFOLIO_SUMMARY',
                'fqn': 'DBT_DEMO.DEV.FACT_PORTFOLIO_SUMMARY',
                'row_count': 5000,
                'content_hash': 12345678,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
        ]
        
        # Save baseline with both build and query metrics
        saved = save_baseline(
            pipeline_name='Pipeline A',
            build_metrics=example_build_metrics,
            query_metrics=example_query_metrics,
            fingerprints=example_fingerprints,
        )
        logger.info(f"Baseline saved: {saved}")
        
        # Load baseline
        loaded = load_baseline(pipeline_name='Pipeline A')
        logger.info(f"Loaded baseline: {loaded is not None}")
        
        # Validate baseline
        is_valid, message = validate_baseline(loaded)
        logger.info(f"Baseline validation: {is_valid} - {message}")
        
        # Clear baseline
        cleared = clear_baseline(pipeline_name='Pipeline A')
        logger.info(f"Baseline cleared: {cleared}")
    
    except Exception as e:
        logger.error(f"Error in example: {e}", exc_info=True)
        sys.exit(1)
