"""
dbt Pipeline Execution Wrapper

This module provides functionality to execute dbt pipelines with tag-based model selection,
capture execution metadata, and return structured results.
"""

import json
import logging
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Dict, Any

import yaml


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
class PipelineRunResult:
    """Structured result object for a pipeline run."""
    pipeline_id: str
    success: bool
    executed_models: List[str] = field(default_factory=list)
    fqn_models: List[str] = field(default_factory=list)
    start_timestamp: str = ""
    end_timestamp: str = ""
    error_message: Optional[str] = None
    compile_only: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            'pipeline_id': self.pipeline_id,
            'success': self.success,
            'executed_models': self.executed_models,
            'fqn_models': self.fqn_models,
            'start_timestamp': self.start_timestamp,
            'end_timestamp': self.end_timestamp,
            'error_message': self.error_message,
            'compile_only': self.compile_only,
        }


class PipelineExecutor:
    """Executes dbt pipelines with tag-based model selection."""
    
    def __init__(self, snowflake_dir: Optional[Path] = None):
        """
        Initialize the pipeline executor.
        
        Args:
            snowflake_dir: Path to snowflake directory (default: current directory)
        """
        if snowflake_dir is None:
            # Find snowflake directory (go up from benchmark if needed)
            current_dir = Path.cwd()
            if current_dir.name == 'benchmark':
                self.snowflake_dir = current_dir.parent
            else:
                self.snowflake_dir = current_dir
        else:
            self.snowflake_dir = snowflake_dir
        
        self.benchmark_dir = self.snowflake_dir / 'benchmark'
        self.config_file = self.benchmark_dir / 'benchmark_config.yml'
        self.profiles_file = self.snowflake_dir / 'profiles.yml'
        self.dbt_project_file = self.snowflake_dir / 'dbt_project.yml'
        self.target_dir = self.snowflake_dir / 'target'
        
        logger.info(f"Initialized PipelineExecutor with snowflake_dir={self.snowflake_dir}")
        
        self._pipeline_config: Dict[str, Any] = {}
        self._profiles_config: Dict[str, Any] = {}
        self._database: str = ""
        self._schema: str = ""
        
        self._load_configurations()
    
    def _load_configurations(self) -> None:
        """Load benchmark config and profiles config."""
        logger.info("Loading configuration files...")
        
        # Load benchmark config
        if not self.config_file.exists():
            raise FileNotFoundError(f"benchmark_config.yml not found at {self.config_file}")
        
        with open(self.config_file, 'r') as f:
            config = yaml.safe_load(f)
            self._pipeline_config = config.get('benchmarking', {})
            logger.debug(f"Loaded benchmark config: {self._pipeline_config}")
        
        # Load profiles config
        if not self.profiles_file.exists():
            raise FileNotFoundError(f"profiles.yml not found at {self.profiles_file}")
        
        with open(self.profiles_file, 'r') as f:
            profiles = yaml.safe_load(f)
            self._profiles_config = profiles
            
            # Extract database and schema from the dev output
            try:
                dev_config = profiles['bain_capital_analytics']['outputs']['dev']
                self._database = dev_config['database']
                self._schema = dev_config['schema']
                logger.info(f"Loaded database: {self._database}, schema: {self._schema}")
            except (KeyError, TypeError) as e:
                logger.warning(f"Could not extract database/schema from profiles.yml: {e}")
    
    def _get_pipeline_selector(self, pipeline_id: str) -> str:
        """
        Get the dbt selector string for a pipeline.
        
        Args:
            pipeline_id: 'A', 'B', 'C', or 'all'
            
        Returns:
            dbt selector string
        """
        if pipeline_id == 'all':
            return "tag:pipeline_a tag:pipeline_b tag:pipeline_c"
        
        # Map pipeline ID to selector
        pipeline_map = {
            'A': 'tag:pipeline_a',
            'B': 'tag:pipeline_b',
            'C': 'tag:pipeline_c',
        }
        
        if pipeline_id not in pipeline_map:
            raise ValueError(f"Invalid pipeline_id: {pipeline_id}. Must be 'A', 'B', 'C', or 'all'")
        
        return pipeline_map[pipeline_id]
    
    def _execute_dbt_command(
        self,
        command: List[str],
        start_time: datetime
    ) -> tuple[bool, str, List[str]]:
        """
        Execute a dbt command via subprocess.
        
        Args:
            command: dbt command to execute (e.g., ['dbt', 'compile', '--select', 'tag:pipeline_a'])
            start_time: Command start time
            
        Returns:
            Tuple of (success, output, executed_models)
        """
        try:
            logger.info(f"Executing: {' '.join(command)}")
            result = subprocess.run(
                command,
                cwd=str(self.snowflake_dir),
                capture_output=True,
                text=True,
                check=False
            )
            
            # Log dbt output at DEBUG level
            if result.stdout:
                logger.debug(f"dbt stdout:\n{result.stdout}")
            if result.stderr:
                logger.debug(f"dbt stderr:\n{result.stderr}")
            
            success = result.returncode == 0
            output = result.stdout + result.stderr
            
            # Try to get executed models from manifest
            executed_models = self._get_executed_models_from_manifest(tag_filter=self._current_tag)

            return success, output, executed_models
        
        except Exception as e:
            logger.error(f"Error executing dbt command: {e}", exc_info=True)
            return False, str(e), []
    
    def _get_executed_models_from_manifest(self, tag_filter: str = None) -> List[str]:
        """
        Parse the dbt manifest to get list of executed models.

        Args:
            tag_filter: Optional tag to filter models (e.g., 'pipeline_a')

        Returns:
            List of executed model names
        """
        manifest_path = self.target_dir / 'manifest.json'

        if not manifest_path.exists():
            logger.warning(f"manifest.json not found at {manifest_path}")
            return []

        try:
            with open(manifest_path, 'r') as f:
                manifest = json.load(f)

            # Get all nodes that are models (type='model')
            models = []
            for node_id, node in manifest.get('nodes', {}).items():
                if node.get('resource_type') == 'model':
                    model_name = node.get('name')
                    if model_name:
                        # Filter by tag if specified
                        if tag_filter:
                            node_tags = node.get('tags', []) or node.get('config', {}).get('tags', [])
                            if tag_filter not in node_tags:
                                continue
                        models.append(model_name)

            logger.debug(f"Found {len(models)} models in manifest (tag_filter={tag_filter})")
            return sorted(models)
        
        except (json.JSONDecodeError, KeyError, IOError) as e:
            logger.warning(f"Error parsing manifest.json: {e}")
            return []
    
    def _convert_to_fqn(self, model_names: List[str]) -> List[str]:
        """
        Convert model names to fully qualified Snowflake names.
        
        Args:
            model_names: List of dbt model names
            
        Returns:
            List of FQNs in format: DATABASE.SCHEMA.MODEL_NAME
        """
        if not self._database or not self._schema:
            logger.warning("Database or schema not configured, returning model names as-is")
            return model_names
        
        fqns = [f"{self._database}.{self._schema}.{model.upper()}" for model in model_names]
        logger.debug(f"Converted {len(model_names)} models to FQNs")
        return fqns
    
    def run_pipeline(
        self,
        pipeline_id: str,
        compile_only: bool = False
    ) -> PipelineRunResult:
        """
        Execute a dbt pipeline with tag-based model selection.
        
        Args:
            pipeline_id: 'A', 'B', 'C', or 'all'
            compile_only: If True, only run compile (no execution)
            
        Returns:
            PipelineRunResult object with execution metadata
        """
        # Initialize result
        result = PipelineRunResult(
            pipeline_id=pipeline_id,
            success=False,
            compile_only=compile_only
        )
        
        # Capture start timestamp
        start_time = datetime.now(timezone.utc)
        result.start_timestamp = start_time.isoformat()
        logger.info(f"Starting pipeline {pipeline_id} execution at {result.start_timestamp}")
        
        try:
            # Get selector for this pipeline
            selector = self._get_pipeline_selector(pipeline_id)
            # Set tag filter for manifest parsing
            self._current_tag = f"pipeline_{pipeline_id.lower()}" if pipeline_id != 'all' else None
            logger.info(f"Using selector: {selector}")
            
            # Step 1: Execute dbt compile
            logger.info("Step 1: Running dbt compile...")
            compile_cmd = ['dbt', 'compile', '--select', selector]
            compile_success, compile_output, compile_models = self._execute_dbt_command(
                compile_cmd,
                start_time
            )
            
            if not compile_success:
                error_msg = "dbt compile failed"
                logger.error(error_msg)
                result.error_message = compile_output
                result.end_timestamp = datetime.now(timezone.utc).isoformat()
                return result
            
            logger.info(f"dbt compile succeeded. Found {len(compile_models)} models.")
            result.executed_models = compile_models
            
            # If compile_only mode, return here
            if compile_only:
                logger.info("Compile-only mode: skipping dbt run")
                result.success = True
                result.end_timestamp = datetime.now(timezone.utc).isoformat()
                result.fqn_models = self._convert_to_fqn(result.executed_models)
                logger.info(f"Pipeline {pipeline_id} compile completed successfully")
                return result
            
            # Step 2: Execute dbt run
            logger.info("Step 2: Running dbt run...")
            run_cmd = ['dbt', 'run', '--select', selector]
            run_success, run_output, run_models = self._execute_dbt_command(
                run_cmd,
                start_time
            )
            
            if not run_success:
                error_msg = "dbt run failed"
                logger.error(error_msg)
                result.error_message = run_output
                result.end_timestamp = datetime.now(timezone.utc).isoformat()
                return result
            
            logger.info(f"dbt run succeeded. Executed {len(run_models)} models.")
            result.executed_models = run_models
            
            # Convert to FQNs
            result.fqn_models = self._convert_to_fqn(result.executed_models)
            
            # Mark as successful
            result.success = True
            logger.info(f"Pipeline {pipeline_id} execution completed successfully")
            
        except Exception as e:
            logger.error(f"Unexpected error during pipeline execution: {e}", exc_info=True)
            result.error_message = str(e)
        
        finally:
            # Capture end timestamp
            result.end_timestamp = datetime.now(timezone.utc).isoformat()
            logger.info(f"Pipeline {pipeline_id} execution finished at {result.end_timestamp}")
        
        return result


def run_pipeline(
    pipeline_id: str,
    compile_only: bool = False
) -> PipelineRunResult:
    """
    Execute a dbt pipeline with tag-based model selection.
    
    Main entry point for pipeline execution.
    
    Args:
        pipeline_id: 'A', 'B', 'C', or 'all'
        compile_only: If True, only run dbt compile (validation mode)
        
    Returns:
        PipelineRunResult object containing:
            - pipeline_id: Input pipeline identifier
            - success: Whether execution was successful
            - executed_models: List of dbt model names
            - fqn_models: List of fully qualified Snowflake names
            - start_timestamp: ISO format start time
            - end_timestamp: ISO format end time
            - error_message: Error details if failed
            - compile_only: Whether this was a compile-only run
            
    Raises:
        ValueError: If pipeline_id is invalid
        FileNotFoundError: If required configuration files are missing
    """
    executor = PipelineExecutor()
    return executor.run_pipeline(pipeline_id, compile_only=compile_only)


if __name__ == '__main__':
    # Example usage
    logger.info("Running pipeline example")
    
    try:
        result = run_pipeline('A')
        logger.info(f"Pipeline result: {result.to_dict()}")
        
        if result.success:
            logger.info("Pipeline execution successful!")
            logger.info(f"Executed models: {result.executed_models}")
            logger.info(f"FQN models: {result.fqn_models}")
        else:
            logger.error(f"Pipeline execution failed: {result.error_message}")
    
    except Exception as e:
        logger.error(f"Failed to run pipeline: {e}", exc_info=True)
        sys.exit(1)
