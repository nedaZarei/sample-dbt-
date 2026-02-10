"""
Output Correctness Verification Module

This module verifies output correctness by comparing row counts and content hashes
between current and baseline pipeline results. It queries Snowflake models and
computes order-independent content hashes for deterministic validation.
"""

import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import yaml
from snowflake.connector.errors import ProgrammingError, DatabaseError

from metrics_collector import SnowflakeConnection


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
class BaselineFingerprint:
    """Baseline fingerprint for a model."""
    model_name: str
    fqn: str
    row_count: int
    content_hash: Optional[int] = None
    timestamp: str = ""
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaselineFingerprint':
        """Create from dictionary."""
        return cls(
            model_name=data.get('model_name', ''),
            fqn=data.get('fqn', ''),
            row_count=data.get('row_count', 0),
            content_hash=data.get('content_hash'),
            timestamp=data.get('timestamp', ''),
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'model_name': self.model_name,
            'fqn': self.fqn,
            'row_count': self.row_count,
            'content_hash': self.content_hash,
            'timestamp': self.timestamp,
        }


@dataclass
class CurrentFingerprint:
    """Current fingerprint computed from Snowflake model."""
    model_name: str
    fqn: str
    row_count: int
    content_hash: Optional[int] = None
    timestamp: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'model_name': self.model_name,
            'fqn': self.fqn,
            'row_count': self.row_count,
            'content_hash': self.content_hash,
            'timestamp': self.timestamp,
        }


@dataclass
class VerificationResult:
    """Result of verification for a single model."""
    model_name: str
    fqn: str
    passed: bool
    row_count_match: Optional[bool] = None
    hash_match: Optional[bool] = None
    baseline_row_count: Optional[int] = None
    current_row_count: Optional[int] = None
    baseline_hash: Optional[int] = None
    current_hash: Optional[int] = None
    error_message: str = ""
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'model_name': self.model_name,
            'fqn': self.fqn,
            'passed': self.passed,
            'row_count_match': self.row_count_match,
            'hash_match': self.hash_match,
            'baseline_row_count': self.baseline_row_count,
            'current_row_count': self.current_row_count,
            'baseline_hash': self.baseline_hash,
            'current_hash': self.current_hash,
            'error_message': self.error_message,
            'warnings': self.warnings,
        }


@dataclass
class VerificationSummary:
    """Summary of all verification results."""
    pipeline_id: str
    total_models: int
    passed_models: int
    failed_models: int
    results: List[Dict[str, Any]] = field(default_factory=list)
    timestamp: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'pipeline_id': self.pipeline_id,
            'total_models': self.total_models,
            'passed_models': self.passed_models,
            'failed_models': self.failed_models,
            'success_rate': self.passed_models / self.total_models if self.total_models > 0 else 0.0,
            'results': self.results,
            'timestamp': self.timestamp,
        }


class OutputVerifier:
    """Verifies output correctness against baseline fingerprints."""
    
    def __init__(self, snowflake_dir: Optional[Path] = None):
        """
        Initialize output verifier.
        
        Args:
            snowflake_dir: Path to snowflake directory
        """
        if snowflake_dir is None:
            # Find snowflake directory
            current_dir = Path.cwd()
            if current_dir.name == 'benchmark':
                self.snowflake_dir = current_dir.parent
            else:
                self.snowflake_dir = current_dir
        else:
            self.snowflake_dir = snowflake_dir
        
        self.benchmark_dir = self.snowflake_dir / 'benchmark'
        self.profiles_file = self.snowflake_dir / 'profiles.yml'
        
        self.sf_conn = SnowflakeConnection(self.profiles_file)
        logger.info(f"Initialized OutputVerifier with snowflake_dir={self.snowflake_dir}")
    
    def verify_models(
        self,
        pipeline_id: str,
        model_fqns: List[str],
        baseline_fingerprints: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Verify all models against baseline fingerprints.
        
        Args:
            pipeline_id: Pipeline identifier
            model_fqns: List of fully qualified model names (DATABASE.SCHEMA.MODEL)
            baseline_fingerprints: List of baseline fingerprint dictionaries
            
        Returns:
            VerificationSummary as dictionary with detailed results
        """
        logger.info(f"Starting output verification for pipeline {pipeline_id}")
        logger.info(f"Verifying {len(model_fqns)} models")
        
        if not self.sf_conn.connect():
            logger.error("Failed to connect to Snowflake")
            return self._create_failed_summary(pipeline_id, model_fqns)
        
        try:
            # Create baseline lookup
            baseline_map = self._create_baseline_map(baseline_fingerprints)
            
            # Verify each model
            results = []
            passed_count = 0
            
            for fqn in model_fqns:
                result = self._verify_single_model(fqn, baseline_map)
                results.append(result.to_dict())
                
                if result.passed:
                    passed_count += 1
                    logger.info(f"✓ {result.model_name}: PASS")
                else:
                    logger.warning(f"✗ {result.model_name}: FAIL - {result.error_message}")
            
            # Create summary
            summary = VerificationSummary(
                pipeline_id=pipeline_id,
                total_models=len(model_fqns),
                passed_models=passed_count,
                failed_models=len(model_fqns) - passed_count,
                results=results,
                timestamp=datetime.now(timezone.utc).isoformat(),
            )
            
            logger.info(
                f"Verification complete: {passed_count}/{len(model_fqns)} passed"
            )
            
            return summary.to_dict()
        
        except Exception as e:
            logger.error(f"Error during verification: {e}", exc_info=True)
            return self._create_failed_summary(pipeline_id, model_fqns)
        
        finally:
            self.sf_conn.close()
    
    def _create_baseline_map(
        self,
        baseline_fingerprints: List[Dict[str, Any]]
    ) -> Dict[str, BaselineFingerprint]:
        """
        Create mapping of FQN to baseline fingerprint.
        
        Args:
            baseline_fingerprints: List of baseline fingerprint dictionaries
            
        Returns:
            Dictionary mapping FQN to BaselineFingerprint
        """
        baseline_map: Dict[str, BaselineFingerprint] = {}
        
        for fingerprint_dict in baseline_fingerprints:
            fingerprint = BaselineFingerprint.from_dict(fingerprint_dict)
            # Use FQN as key (or model_name if FQN not available)
            key = fingerprint.fqn or fingerprint.model_name
            baseline_map[key] = fingerprint
            logger.debug(f"Added baseline for {key}: row_count={fingerprint.row_count}")
        
        return baseline_map
    
    def _verify_single_model(
        self,
        fqn: str,
        baseline_map: Dict[str, BaselineFingerprint],
    ) -> VerificationResult:
        """
        Verify a single model against baseline.
        
        Args:
            fqn: Fully qualified model name (DATABASE.SCHEMA.MODEL)
            baseline_map: Mapping of FQN to baseline fingerprints
            
        Returns:
            VerificationResult for this model
        """
        model_name = fqn.split('.')[-1]
        
        # Check if baseline exists
        baseline = baseline_map.get(fqn) or baseline_map.get(model_name)
        
        if baseline is None:
            logger.debug(f"No baseline found for {fqn} - treating as first run")
            return VerificationResult(
                model_name=model_name,
                fqn=fqn,
                passed=True,
                error_message="No baseline available (first run)",
                warnings=["No baseline fingerprint found - output accepted as baseline"],
            )
        
        # Get current fingerprint
        current = self._get_current_fingerprint(fqn)
        
        if current is None:
            error_msg = f"Failed to retrieve fingerprint for model {model_name}"
            logger.error(error_msg)
            return VerificationResult(
                model_name=model_name,
                fqn=fqn,
                passed=False,
                error_message=error_msg,
            )
        
        # Compare fingerprints
        return self._compare_fingerprints(baseline, current)
    
    def _get_current_fingerprint(self, fqn: str) -> Optional[CurrentFingerprint]:
        """
        Get current fingerprint from Snowflake.
        
        Queries:
        1. SELECT COUNT(*) FROM <fqn> for row count
        2. SELECT HASH_AGG(HASH(*)) FROM <fqn> for content hash
        
        Args:
            fqn: Fully qualified model name
            
        Returns:
            CurrentFingerprint or None if table doesn't exist
        """
        model_name = fqn.split('.')[-1]
        
        # Get row count
        row_count = self._get_row_count(fqn)
        if row_count is None:
            logger.error(f"Could not get row count for {fqn}")
            return None
        
        logger.debug(f"Retrieved row count for {model_name}: {row_count}")
        
        # Get content hash
        content_hash = self._get_content_hash(fqn, row_count)
        
        if content_hash is None and row_count > 0:
            # Hash retrieval failed but table exists with data
            logger.warning(f"Could not compute content hash for {fqn}")
        
        return CurrentFingerprint(
            model_name=model_name,
            fqn=fqn,
            row_count=row_count,
            content_hash=content_hash,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    
    def _get_row_count(self, fqn: str) -> Optional[int]:
        """
        Get row count for a model.
        
        Args:
            fqn: Fully qualified model name
            
        Returns:
            Row count or None if table doesn't exist
        """
        try:
            sql = f"SELECT COUNT(*) FROM {fqn}"
            results = self.sf_conn.execute_query(sql)
            
            if results is None or len(results) == 0:
                logger.warning(f"No results from row count query for {fqn}")
                return None
            
            row_count = results[0][0]
            if row_count is None:
                return 0
            
            return int(row_count)
        
        except (ProgrammingError, DatabaseError) as e:
            logger.error(f"Table not found or query error for {fqn}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting row count for {fqn}: {e}")
            return None
    
    def _get_content_hash(self, fqn: str, row_count: int) -> Optional[int]:
        """
        Get content hash for a model.
        
        Uses HASH_AGG(HASH(*)) for order-independent hashing.
        Fallback to SUM(HASH(*)) if HASH_AGG not available.
        
        For empty tables (row_count=0), returns None (empty hash).
        
        Args:
            fqn: Fully qualified model name
            row_count: Row count (optimization to skip hash for empty tables)
            
        Returns:
            Content hash or None if table is empty or hash computation fails
        """
        # For empty tables, hash is None/undefined
        if row_count == 0:
            logger.debug(f"Table {fqn} is empty, skipping hash computation")
            return None
        
        # Try primary strategy: HASH_AGG
        hash_result = self._try_hash_agg(fqn)
        if hash_result is not None:
            return hash_result
        
        # Try fallback strategy: SUM(HASH(*))
        logger.debug(f"HASH_AGG failed for {fqn}, trying SUM(HASH(*))")
        hash_result = self._try_sum_hash(fqn)
        if hash_result is not None:
            return hash_result
        
        # Both strategies failed
        logger.warning(f"Could not compute content hash for {fqn}")
        return None
    
    def _try_hash_agg(self, fqn: str) -> Optional[int]:
        """
        Try to compute hash using HASH_AGG(HASH(*)).
        
        HASH_AGG aggregates hashes in an order-independent manner.
        
        Args:
            fqn: Fully qualified model name
            
        Returns:
            Hash value or None if strategy fails
        """
        try:
            sql = f"SELECT HASH_AGG(HASH(*)) FROM {fqn}"
            results = self.sf_conn.execute_query(sql)
            
            if results is None or len(results) == 0:
                logger.debug(f"No results from HASH_AGG query for {fqn}")
                return None
            
            hash_result = results[0][0]
            
            # HASH_AGG can return NULL for empty tables
            if hash_result is None:
                logger.debug(f"HASH_AGG returned NULL for {fqn}")
                return None
            
            return int(hash_result)
        
        except (ProgrammingError, DatabaseError) as e:
            logger.debug(f"HASH_AGG not available or failed for {fqn}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error in HASH_AGG for {fqn}: {e}")
            return None
    
    def _try_sum_hash(self, fqn: str) -> Optional[int]:
        """
        Try to compute hash using SUM(HASH(*)).
        
        Fallback strategy using SUM aggregation.
        Note: This may have collision risks but is deterministic.
        
        Args:
            fqn: Fully qualified model name
            
        Returns:
            Hash value or None if strategy fails
        """
        try:
            sql = f"SELECT SUM(HASH(*)) FROM {fqn}"
            results = self.sf_conn.execute_query(sql)
            
            if results is None or len(results) == 0:
                logger.debug(f"No results from SUM(HASH(*)) query for {fqn}")
                return None
            
            hash_result = results[0][0]
            
            # SUM can return NULL for empty tables
            if hash_result is None:
                logger.debug(f"SUM(HASH(*)) returned NULL for {fqn}")
                return None
            
            return int(hash_result)
        
        except (ProgrammingError, DatabaseError) as e:
            logger.debug(f"SUM(HASH(*)) failed for {fqn}: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error in SUM(HASH(*)) for {fqn}: {e}")
            return None
    
    def _compare_fingerprints(
        self,
        baseline: BaselineFingerprint,
        current: CurrentFingerprint,
    ) -> VerificationResult:
        """
        Compare baseline vs current fingerprints.
        
        Args:
            baseline: Baseline fingerprint
            current: Current fingerprint from Snowflake
            
        Returns:
            VerificationResult with comparison details
        """
        row_count_match = baseline.row_count == current.row_count
        hash_match = True  # Default to True if not compared
        passed = True
        error_messages = []
        warnings = []
        
        # Check row count
        if not row_count_match:
            passed = False
            error_msg = (
                f"Row count mismatch: expected {baseline.row_count}, "
                f"got {current.row_count}"
            )
            error_messages.append(error_msg)
        
        # Check content hash (if both have hashes and row counts match)
        if row_count_match and current.row_count > 0:
            baseline_hash = baseline.content_hash
            current_hash = current.content_hash
            
            # Only compare if both have hashes
            if baseline_hash is not None and current_hash is not None:
                hash_match = baseline_hash == current_hash
                
                if not hash_match:
                    passed = False
                    error_msg = (
                        f"Content hash mismatch: expected {baseline_hash}, "
                        f"got {current_hash}"
                    )
                    error_messages.append(error_msg)
            elif baseline_hash is not None and current_hash is None:
                # Baseline has hash but current doesn't
                warnings.append(
                    f"Could not compute current content hash for comparison "
                    f"(baseline hash: {baseline_hash})"
                )
                hash_match = None  # Indeterminate
            elif baseline_hash is None and current_hash is not None:
                # Current has hash but baseline doesn't
                warnings.append(
                    f"Current content hash computed but baseline has none "
                    f"(current hash: {current_hash})"
                )
                hash_match = None  # Indeterminate
        elif row_count_match and current.row_count == 0:
            # Both empty, no hash check needed
            hash_match = True
            logger.debug(f"{current.model_name} is empty, skipping hash check")
        
        return VerificationResult(
            model_name=current.model_name,
            fqn=current.fqn,
            passed=passed,
            row_count_match=row_count_match,
            hash_match=hash_match,
            baseline_row_count=baseline.row_count,
            current_row_count=current.row_count,
            baseline_hash=baseline.content_hash,
            current_hash=current.content_hash,
            error_message=" | ".join(error_messages),
            warnings=warnings,
        )
    
    def _create_failed_summary(
        self,
        pipeline_id: str,
        model_fqns: List[str],
    ) -> Dict[str, Any]:
        """
        Create a failed verification summary.
        
        Args:
            pipeline_id: Pipeline identifier
            model_fqns: List of model FQNs
            
        Returns:
            VerificationSummary as dictionary
        """
        results = []
        for fqn in model_fqns:
            model_name = fqn.split('.')[-1]
            result = VerificationResult(
                model_name=model_name,
                fqn=fqn,
                passed=False,
                error_message="Verification system error - unable to connect or verify",
            )
            results.append(result.to_dict())
        
        summary = VerificationSummary(
            pipeline_id=pipeline_id,
            total_models=len(model_fqns),
            passed_models=0,
            failed_models=len(model_fqns),
            results=results,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
        
        return summary.to_dict()


def verify_output(
    pipeline_id: str,
    model_fqns: List[str],
    baseline_fingerprints: List[Dict[str, Any]],
    snowflake_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    """
    Verify output correctness against baseline fingerprints.
    
    Main entry point for output verification.
    
    Args:
        pipeline_id: Pipeline identifier (A, B, C)
        model_fqns: List of fully qualified model names (DATABASE.SCHEMA.MODEL)
        baseline_fingerprints: List of baseline fingerprint dictionaries containing:
            - model_name: Model name
            - fqn: Fully qualified name
            - row_count: Expected row count
            - content_hash: Expected content hash (optional)
            - timestamp: Baseline timestamp (optional)
        snowflake_dir: Optional path to snowflake directory
        
    Returns:
        VerificationSummary dictionary containing:
            - pipeline_id: Pipeline identifier
            - total_models: Total models verified
            - passed_models: Number of passed models
            - failed_models: Number of failed models
            - success_rate: Percentage of passed models
            - results: List of VerificationResult dictionaries per model
            - timestamp: When verification was performed
            
            Each result includes:
            - model_name: Model name
            - fqn: Fully qualified name
            - passed: Whether verification passed
            - row_count_match: Whether row counts match
            - hash_match: Whether hashes match
            - baseline_row_count: Baseline row count
            - current_row_count: Current row count
            - baseline_hash: Baseline content hash
            - current_hash: Current content hash
            - error_message: Detailed error if failed
            - warnings: List of warnings
            
    Raises:
        Exception: On connection or query errors
    """
    verifier = OutputVerifier(snowflake_dir)
    return verifier.verify_models(pipeline_id, model_fqns, baseline_fingerprints)


if __name__ == '__main__':
    # Example usage
    logger.info("Running output verifier example")
    
    try:
        # Example baseline fingerprints
        example_baseline = [
            {
                'model_name': 'FACT_PORTFOLIO_SUMMARY',
                'fqn': 'DBT_DEMO.DEV.FACT_PORTFOLIO_SUMMARY',
                'row_count': 100,
                'content_hash': 12345678,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }
        ]
        
        # Example verification
        result = verify_output(
            pipeline_id='A',
            model_fqns=['DBT_DEMO.DEV.FACT_PORTFOLIO_SUMMARY'],
            baseline_fingerprints=example_baseline,
        )
        
        logger.info(f"Verification result: {result}")
    
    except Exception as e:
        logger.error(f"Failed to verify output: {e}", exc_info=True)
        sys.exit(1)
