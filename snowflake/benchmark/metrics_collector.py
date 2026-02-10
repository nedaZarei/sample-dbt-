"""
Snowflake Metrics Collector

This module collects query performance metrics from Snowflake query history
and aggregates them at the pipeline level. It connects to Snowflake using
credentials from dbt profiles.yml or environment variables.
"""

import logging
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple

import yaml
from snowflake.connector import connect
from snowflake.connector.errors import ProgrammingError, DatabaseError


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
class QueryMetrics:
    """Metrics extracted from a single query."""
    query_id: str
    query_text: str
    execution_time_ms: int
    bytes_scanned: int
    rows_produced: int
    partitions_scanned: Optional[int] = None
    partitions_total: Optional[int] = None
    user_name: str = ""
    warehouse_name: str = ""
    database_name: str = ""
    schema_name: str = ""
    execution_time: str = ""  # ISO format timestamp
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'query_id': self.query_id,
            'execution_time_ms': self.execution_time_ms,
            'bytes_scanned': self.bytes_scanned,
            'rows_produced': self.rows_produced,
            'partitions_scanned': self.partitions_scanned,
            'partitions_total': self.partitions_total,
        }


@dataclass
class ModelMetrics:
    """Aggregated metrics for a single model."""
    model_name: str
    fqn: str
    total_execution_time_ms: int = 0
    total_bytes_scanned: int = 0
    total_rows_produced: int = 0
    query_count: int = 0
    queries: List[QueryMetrics] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'model_name': self.model_name,
            'fqn': self.fqn,
            'total_execution_time_ms': self.total_execution_time_ms,
            'total_bytes_scanned': self.total_bytes_scanned,
            'total_rows_produced': self.total_rows_produced,
            'query_count': self.query_count,
        }


@dataclass
class PipelineMetrics:
    """Aggregated metrics for entire pipeline."""
    pipeline_id: str
    timestamp: str
    total_execution_time_ms: int = 0
    total_bytes_scanned: int = 0
    total_rows_produced: int = 0
    model_metrics: List[Dict[str, Any]] = field(default_factory=list)
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    snowflake_version: Optional[str] = None
    collection_timestamp: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'pipeline_id': self.pipeline_id,
            'timestamp': self.timestamp,
            'total_execution_time_ms': self.total_execution_time_ms,
            'total_bytes_scanned': self.total_bytes_scanned,
            'total_rows_produced': self.total_rows_produced,
            'model_metrics': self.model_metrics,
            'warehouse': self.warehouse,
            'database': self.database,
            'schema': self.schema,
            'snowflake_version': self.snowflake_version,
            'collection_timestamp': self.collection_timestamp,
        }


class SnowflakeConnection:
    """Manages Snowflake connection using profiles.yml or environment variables."""
    
    def __init__(self, profiles_file: Optional[Path] = None, profile_name: str = "bain_capital_analytics"):
        """
        Initialize Snowflake connection handler.
        
        Args:
            profiles_file: Path to profiles.yml (default: loads from standard location)
            profile_name: dbt profile name to use
        """
        self.profiles_file = profiles_file
        self.profile_name = profile_name
        self.connection = None
        self._conn_params: Dict[str, Any] = {}
        self._load_credentials()
    
    def _load_credentials(self) -> None:
        """Load credentials from profiles.yml or environment variables."""
        logger.info("Loading Snowflake credentials...")
        
        # Try to load from profiles.yml first
        if self.profiles_file is None:
            # Look for default location
            home = Path.home()
            default_profiles = home / '.dbt' / 'profiles.yml'
            
            if default_profiles.exists():
                self.profiles_file = default_profiles
            else:
                # Try current directory structure
                current_dir = Path.cwd()
                if current_dir.name == 'benchmark':
                    self.profiles_file = current_dir.parent / 'profiles.yml'
                elif (current_dir / 'snowflake' / 'profiles.yml').exists():
                    self.profiles_file = current_dir / 'snowflake' / 'profiles.yml'
        
        if self.profiles_file and self.profiles_file.exists():
            logger.debug(f"Loading credentials from {self.profiles_file}")
            self._load_from_profiles_yml()
        else:
            logger.info("profiles.yml not found, falling back to environment variables")
            self._load_from_env_vars()
    
    def _load_from_profiles_yml(self) -> None:
        """Load credentials from dbt profiles.yml."""
        try:
            with open(self.profiles_file, 'r') as f:
                profiles = yaml.safe_load(f)
            
            if not profiles or self.profile_name not in profiles:
                logger.warning(f"Profile '{self.profile_name}' not found in profiles.yml")
                self._load_from_env_vars()
                return
            
            profile = profiles[self.profile_name]
            outputs = profile.get('outputs', {})
            dev_config = outputs.get('dev', {})
            
            # Extract credentials
            # Note: profiles.yml may use environment variable references like {{ env_var('VAR') }}
            account = dev_config.get('account', '')
            user = dev_config.get('user', '')
            password = dev_config.get('password', '')
            database = dev_config.get('database', '')
            schema = dev_config.get('schema', '')
            warehouse = dev_config.get('warehouse', '')
            role = dev_config.get('role', '')
            
            # Check if these are environment variable references
            account = self._resolve_env_var(account)
            user = self._resolve_env_var(user)
            password = self._resolve_env_var(password)
            
            if account and user and password:
                self._conn_params = {
                    'account': account,
                    'user': user,
                    'password': password,
                    'warehouse': warehouse or 'COMPUTE_WH',
                    'database': database,
                    'schema': schema,
                    'role': role,
                }
                logger.info(f"Loaded credentials from profiles.yml: account={account}, user={user}")
            else:
                logger.warning("Could not extract all required credentials from profiles.yml")
                self._load_from_env_vars()
        
        except Exception as e:
            logger.warning(f"Error loading from profiles.yml: {e}")
            self._load_from_env_vars()
    
    def _load_from_env_vars(self) -> None:
        """Load credentials from environment variables."""
        account = os.getenv('SNOWFLAKE_ACCOUNT', '')
        user = os.getenv('SNOWFLAKE_USER', '')
        password = os.getenv('SNOWFLAKE_PASSWORD', '')
        warehouse = os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
        database = os.getenv('SNOWFLAKE_DATABASE', '')
        schema = os.getenv('SNOWFLAKE_SCHEMA', '')
        role = os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        
        if account and user and password:
            self._conn_params = {
                'account': account,
                'user': user,
                'password': password,
                'warehouse': warehouse,
                'database': database,
                'schema': schema,
                'role': role,
            }
            logger.info(f"Loaded credentials from environment variables: account={account}, user={user}")
        else:
            logger.error("Could not load Snowflake credentials from environment variables")
            missing = []
            if not account:
                missing.append('SNOWFLAKE_ACCOUNT')
            if not user:
                missing.append('SNOWFLAKE_USER')
            if not password:
                missing.append('SNOWFLAKE_PASSWORD')
            logger.error(f"Missing required environment variables: {', '.join(missing)}")
    
    @staticmethod
    def _resolve_env_var(value: str) -> str:
        """
        Resolve environment variable reference in {{ env_var('VAR') }} format.
        
        Args:
            value: Value that may contain env_var reference
            
        Returns:
            Resolved value or original value
        """
        if not isinstance(value, str):
            return value
        
        if '{{ env_var(' in value and ')' in value:
            # Extract variable name from {{ env_var('VAR') }}
            start = value.find("('") + 2
            end = value.find("')")
            if start > 1 and end > start:
                var_name = value[start:end]
                resolved = os.getenv(var_name, '')
                logger.debug(f"Resolved env_var('{var_name}') = {resolved[:20]}...")
                return resolved
        
        return value
    
    def connect(self) -> bool:
        """
        Establish connection to Snowflake.
        
        Returns:
            True if successful, False otherwise
        """
        if not self._conn_params or not all([
            self._conn_params.get('account'),
            self._conn_params.get('user'),
            self._conn_params.get('password'),
        ]):
            logger.error("Missing required connection parameters")
            return False
        
        try:
            logger.info(f"Connecting to Snowflake account: {self._conn_params['account']}")
            self.connection = connect(
                account=self._conn_params['account'],
                user=self._conn_params['user'],
                password=self._conn_params['password'],
                warehouse=self._conn_params.get('warehouse'),
                database=self._conn_params.get('database'),
                schema=self._conn_params.get('schema'),
                role=self._conn_params.get('role'),
            )
            logger.info("Successfully connected to Snowflake")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            return False
    
    def close(self) -> None:
        """Close Snowflake connection."""
        if self.connection:
            try:
                self.connection.close()
                logger.debug("Closed Snowflake connection")
            except Exception as e:
                logger.warning(f"Error closing connection: {e}")
    
    def execute_query(self, query: str, params: Optional[List[Any]] = None) -> Optional[List[Tuple]]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            List of result tuples or None if error
        """
        if not self.connection:
            logger.error("Not connected to Snowflake")
            return None
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            results = cursor.fetchall()
            cursor.close()
            return results
        except (ProgrammingError, DatabaseError) as e:
            logger.error(f"Query execution error: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error executing query: {e}")
            return None
    
    def execute_count_queries(
        self,
        models: List[Dict[str, str]],
    ) -> Dict[str, Any]:
        """
        Execute COUNT(*) queries on a list of models and capture execution metadata.
        
        Executes COUNT(*) queries against each model, capturing query IDs via the
        Snowflake connector's sfqid property. Continues execution even if individual
        queries fail.
        
        Args:
            models: List of dicts with 'model_name' and 'fqn' keys
                   Example: [
                       {'model_name': 'fact_portfolio_summary', 'fqn': 'DBT_DEMO.DEV.FACT_PORTFOLIO_SUMMARY'},
                       {'model_name': 'report_portfolio_overview', 'fqn': 'DBT_DEMO.DEV.REPORT_PORTFOLIO_OVERVIEW'}
                   ]
        
        Returns:
            Dictionary with structure:
            {
                'successful': [
                    {
                        'model_name': 'fact_portfolio_summary',
                        'fqn': 'DBT_DEMO.DEV.FACT_PORTFOLIO_SUMMARY',
                        'query_id': '<query_id>',
                        'row_count': 12345
                    },
                    ...
                ],
                'failed': [
                    {
                        'model_name': 'some_model',
                        'fqn': 'DBT_DEMO.DEV.SOME_MODEL',
                        'error': 'Error message'
                    },
                    ...
                ],
                'total_executed': 5,
                'total_successful': 4,
                'total_failed': 1
            }
        """
        if not self.connection:
            logger.error("Not connected to Snowflake")
            return {
                'successful': [],
                'failed': [
                    {
                        'model_name': m.get('model_name', 'unknown'),
                        'fqn': m.get('fqn', 'unknown'),
                        'error': 'Connection not established'
                    }
                    for m in models
                ],
                'total_executed': 0,
                'total_successful': 0,
                'total_failed': len(models),
            }
        
        successful = []
        failed = []
        
        for model in models:
            model_name = model.get('model_name', 'unknown')
            fqn = model.get('fqn', 'unknown')
            
            try:
                # Build COUNT query
                count_query = f"SELECT COUNT(*) FROM {fqn}"
                logger.debug(f"Executing COUNT query for {model_name}: {count_query}")
                
                # Execute query
                cursor = self.connection.cursor()
                cursor.execute(count_query)
                
                # Capture query_id from cursor
                query_id = cursor.sfqid
                
                # Fetch result
                result = cursor.fetchone()
                row_count = result[0] if result else 0
                
                cursor.close()
                
                successful.append({
                    'model_name': model_name,
                    'fqn': fqn,
                    'query_id': query_id,
                    'row_count': row_count,
                })
                
                logger.info(f"COUNT query successful for {model_name} (query_id={query_id}, rows={row_count})")
            
            except (ProgrammingError, DatabaseError) as e:
                logger.warning(f"Failed to execute COUNT query for {model_name}: {e}")
                failed.append({
                    'model_name': model_name,
                    'fqn': fqn,
                    'error': str(e),
                })
            
            except Exception as e:
                logger.warning(f"Unexpected error executing COUNT query for {model_name}: {e}")
                failed.append({
                    'model_name': model_name,
                    'fqn': fqn,
                    'error': str(e),
                })
        
        result_dict = {
            'successful': successful,
            'failed': failed,
            'total_executed': len(models),
            'total_successful': len(successful),
            'total_failed': len(failed),
        }
        
        logger.info(
            f"COUNT query execution complete: {len(successful)} successful, {len(failed)} failed"
        )
        
        return result_dict


class MetricsCollector:
    """Collects and aggregates query metrics from Snowflake."""
    
    def __init__(self, snowflake_dir: Optional[Path] = None):
        """
        Initialize metrics collector.
        
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
        self.config_file = self.benchmark_dir / 'benchmark_config.yml'
        
        self.sf_conn = SnowflakeConnection(self.profiles_file)
        self._benchmark_config: Dict[str, Any] = {}
        self._load_benchmark_config()
        
        logger.info(f"Initialized MetricsCollector with snowflake_dir={self.snowflake_dir}")
    
    def _load_benchmark_config(self) -> None:
        """Load benchmark configuration."""
        if not self.config_file.exists():
            logger.warning(f"benchmark_config.yml not found at {self.config_file}")
            return
        
        try:
            with open(self.config_file, 'r') as f:
                config = yaml.safe_load(f)
                self._benchmark_config = config.get('benchmarking', {})
                logger.debug("Loaded benchmark config")
        except Exception as e:
            logger.warning(f"Error loading benchmark config: {e}")
    
    def collect_metrics(
        self,
        pipeline_id: str,
        executed_models: List[str],
        fqn_models: List[str],
        start_timestamp: str,
        end_timestamp: str,
        time_window_minutes: int = 10,
    ) -> Dict[str, Any]:
        """
        Collect and aggregate metrics for a pipeline execution.
        
        Args:
            pipeline_id: Pipeline identifier (A, B, C)
            executed_models: List of executed model names
            fqn_models: List of fully qualified model names
            start_timestamp: Pipeline start time (ISO format)
            end_timestamp: Pipeline end time (ISO format)
            time_window_minutes: Time window to look back for queries
            
        Returns:
            Dictionary with aggregated metrics
        """
        logger.info(f"Collecting metrics for pipeline {pipeline_id}")
        
        if not self.sf_conn.connect():
            logger.error("Failed to connect to Snowflake")
            return self._create_empty_result(pipeline_id, start_timestamp)
        
        try:
            # Parse timestamps
            start_dt = datetime.fromisoformat(start_timestamp.replace('Z', '+00:00'))
            end_dt = datetime.fromisoformat(end_timestamp.replace('Z', '+00:00'))
            
            # Add time window buffer for query history lookback
            query_start_dt = start_dt - timedelta(minutes=time_window_minutes)
            query_end_dt = end_dt + timedelta(minutes=time_window_minutes)
            
            # Get Snowflake version and metadata
            version_info = self._get_snowflake_version()
            
            # Query execution history
            queries = self._query_execution_history(
                query_start_dt,
                query_end_dt,
                executed_models,
            )
            
            if not queries:
                logger.warning("No queries found in execution history")
                return self._create_empty_result(pipeline_id, start_timestamp, version_info)
            
            logger.info(f"Found {len(queries)} queries in history")
            
            # Match queries to models
            model_query_map = self._match_queries_to_models(queries, executed_models, fqn_models)
            
            # Aggregate metrics
            result = self._aggregate_metrics(
                pipeline_id,
                start_timestamp,
                executed_models,
                fqn_models,
                model_query_map,
                version_info,
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error collecting metrics: {e}", exc_info=True)
            return self._create_empty_result(pipeline_id, start_timestamp)
        
        finally:
            self.sf_conn.close()
    
    def _get_snowflake_version(self) -> Dict[str, Any]:
        """Get Snowflake version and account metadata."""
        try:
            cursor = self.sf_conn.connection.cursor()
            cursor.execute("SELECT CURRENT_ACCOUNT(), CURRENT_WAREHOUSE(), CURRENT_DATABASE(), CURRENT_SCHEMA()")
            row = cursor.fetchone()
            cursor.close()
            
            if row:
                return {
                    'account': row[0],
                    'warehouse': row[1],
                    'database': row[2],
                    'schema': row[3],
                }
        except Exception as e:
            logger.debug(f"Could not get Snowflake metadata: {e}")
        
        return {}
    
    def _query_execution_history(
        self,
        start_time: datetime,
        end_time: datetime,
        model_names: List[str],
    ) -> List[Dict[str, Any]]:
        """
        Query INFORMATION_SCHEMA.QUERY_HISTORY for recent queries.
        
        Args:
            start_time: Query start time
            end_time: Query end time
            model_names: List of model names to filter
            
        Returns:
            List of query records
        """
        # Calculate how many minutes ago the time range covers
        now = datetime.now(start_time.tzinfo) if start_time.tzinfo else datetime.utcnow()
        minutes_ago_start = max(int((now - start_time).total_seconds() / 60) + 5, 15)

        # Use Snowflake DATEADD relative to CURRENT_TIMESTAMP to avoid timezone issues
        sql = f"""
        SELECT
            QUERY_ID,
            QUERY_TEXT,
            USER_NAME,
            WAREHOUSE_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            TOTAL_ELAPSED_TIME,
            BYTES_SCANNED,
            ROWS_PRODUCED,
            START_TIME
        FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
            END_TIME_RANGE_START => DATEADD(minute, -{minutes_ago_start}, CURRENT_TIMESTAMP()),
            RESULT_LIMIT => 10000
        ))
        WHERE QUERY_TEXT NOT LIKE '%INFORMATION_SCHEMA%'
            AND QUERY_TEXT NOT LIKE '%QUERY_HISTORY%'
            AND QUERY_TEXT NOT LIKE '%SHOW%'
        ORDER BY START_TIME DESC
        """

        logger.debug(f"Executing query history query looking back {minutes_ago_start} minutes")
        
        try:
            cursor = self.sf_conn.connection.cursor()
            cursor.execute(sql)
            
            # Fetch all results
            columns = [desc[0] for desc in cursor.description]
            results = cursor.fetchall()
            cursor.close()
            
            # Convert to list of dictionaries
            queries = []
            for row in results:
                query_dict = dict(zip(columns, row))
                queries.append(query_dict)
            
            logger.info(f"Retrieved {len(queries)} queries from INFORMATION_SCHEMA.QUERY_HISTORY")
            return queries
        
        except Exception as e:
            logger.error(f"Error querying execution history: {e}")
            return []
    
    def _match_queries_to_models(
        self,
        queries: List[Dict[str, Any]],
        executed_models: List[str],
        fqn_models: List[str],
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Match queries to executed dbt models.
        
        Uses multiple strategies:
        1. Query text contains model name
        2. Query text contains FQN (table name)
        3. Timestamp correlation with execution window
        
        Args:
            queries: List of query records from history
            executed_models: List of executed model names
            fqn_models: List of fully qualified model names
            
        Returns:
            Dictionary mapping model names to their queries
        """
        model_query_map: Dict[str, List[Dict[str, Any]]] = {model: [] for model in executed_models}

        import re

        for query in queries:
            query_text = query.get('QUERY_TEXT', '').upper()

            # Match dbt DDL: "CREATE OR REPLACE VIEW/TABLE SCHEMA.MODEL_NAME"
            # Extract the target model name from the DDL statement
            ddl_match = re.search(
                r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:VIEW|TABLE|TRANSIENT\s+TABLE)\s+\S+\.(\w+)',
                query_text
            )
            if ddl_match:
                target_name = ddl_match.group(1)
                for model in executed_models:
                    if model.upper() == target_name:
                        model_query_map[model].append(query)
                        logger.debug(f"Matched DDL query {query.get('QUERY_ID')} to model {model}")
                        break

        return model_query_map
    
    def _aggregate_metrics(
        self,
        pipeline_id: str,
        start_timestamp: str,
        executed_models: List[str],
        fqn_models: List[str],
        model_query_map: Dict[str, List[Dict[str, Any]]],
        version_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Aggregate metrics at pipeline level.
        
        Args:
            pipeline_id: Pipeline identifier
            start_timestamp: Pipeline start timestamp
            executed_models: List of executed model names
            fqn_models: List of FQN models
            model_query_map: Mapping of models to their queries
            version_info: Snowflake version and metadata
            
        Returns:
            Aggregated pipeline metrics dictionary
        """
        total_execution_time = 0
        total_bytes_scanned = 0
        total_rows_produced = 0
        model_metrics_list = []
        
        for model in executed_models:
            queries = model_query_map.get(model, [])
            
            if not queries:
                logger.debug(f"No queries found for model {model}")
            
            model_execution_time = 0
            model_bytes_scanned = 0
            model_rows_produced = 0
            
            for query in queries:
                elapsed_time = query.get('TOTAL_ELAPSED_TIME', 0) or 0
                bytes_scanned = query.get('BYTES_SCANNED', 0) or 0
                rows_produced = query.get('ROWS_PRODUCED', 0) or 0
                
                model_execution_time += elapsed_time
                model_bytes_scanned += bytes_scanned
                model_rows_produced += rows_produced
            
            # Add to pipeline totals
            total_execution_time += model_execution_time
            total_bytes_scanned += model_bytes_scanned
            total_rows_produced += model_rows_produced
            
            # Get FQN for this model
            model_fqn = None
            for fqn in fqn_models:
                if fqn.endswith(f'.{model.upper()}'):
                    model_fqn = fqn
                    break
            
            model_metrics_list.append({
                'model_name': model,
                'fqn': model_fqn,
                'total_execution_time_ms': model_execution_time,
                'total_bytes_scanned': model_bytes_scanned,
                'total_rows_produced': model_rows_produced,
                'query_count': len(queries),
            })
        
        # Build result dictionary
        result = {
            'pipeline_id': pipeline_id,
            'timestamp': start_timestamp,
            'total_execution_time_ms': total_execution_time,
            'total_bytes_scanned': total_bytes_scanned,
            'total_rows_produced': total_rows_produced,
            'model_details': model_metrics_list,
            'warehouse': version_info.get('warehouse'),
            'database': version_info.get('database'),
            'schema': version_info.get('schema'),
            'collection_timestamp': datetime.now(timezone.utc).isoformat(),
        }
        
        logger.info(
            f"Aggregated metrics for pipeline {pipeline_id}: "
            f"execution_time={total_execution_time}ms, "
            f"bytes={total_bytes_scanned}, "
            f"rows={total_rows_produced}"
        )
        
        return result
    
    def collect_metrics_by_query_ids(
        self,
        pipeline_id: str,
        query_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Collect metrics for queries identified by their query IDs.

        Unlike collect_metrics() which matches DDL statements by regex,
        this method looks up specific query IDs in QUERY_HISTORY directly.
        Used for query-time metrics (e.g., SELECT COUNT(*) on final models).

        Args:
            pipeline_id: Pipeline identifier (A, B, C)
            query_results: List of dicts from execute_count_queries()['successful'],
                          each containing 'model_name', 'fqn', 'query_id', 'row_count'

        Returns:
            Dictionary with aggregated metrics in the same format as collect_metrics()
        """
        logger.info(f"Collecting metrics by query IDs for pipeline {pipeline_id}")

        if not query_results:
            logger.warning("No query results provided")
            return self._create_empty_result(pipeline_id, datetime.now(timezone.utc).isoformat())

        query_ids = [r['query_id'] for r in query_results if r.get('query_id')]

        if not query_ids:
            logger.warning("No query IDs found in results")
            return self._create_empty_result(pipeline_id, datetime.now(timezone.utc).isoformat())

        if not self.sf_conn.connect():
            logger.error("Failed to connect to Snowflake")
            return self._create_empty_result(pipeline_id, datetime.now(timezone.utc).isoformat())

        try:
            # Build a query ID -> model info mapping
            qid_to_model = {}
            for r in query_results:
                if r.get('query_id'):
                    qid_to_model[r['query_id']] = {
                        'model_name': r.get('model_name', 'unknown'),
                        'fqn': r.get('fqn', 'unknown'),
                        'row_count': r.get('row_count', 0),
                    }

            # Query QUERY_HISTORY by specific query IDs
            placeholders = ', '.join([f"'{qid}'" for qid in query_ids])
            sql = f"""
            SELECT
                QUERY_ID,
                QUERY_TEXT,
                TOTAL_ELAPSED_TIME,
                BYTES_SCANNED,
                ROWS_PRODUCED,
                START_TIME
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                RESULT_LIMIT => 10000
            ))
            WHERE QUERY_ID IN ({placeholders})
            """

            logger.debug(f"Looking up {len(query_ids)} query IDs in QUERY_HISTORY")

            cursor = self.sf_conn.connection.cursor()
            cursor.execute(sql)
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            cursor.close()

            # Build query_id -> history record mapping
            history_map = {}
            for row in rows:
                record = dict(zip(columns, row))
                history_map[record['QUERY_ID']] = record

            logger.info(f"Found {len(history_map)} of {len(query_ids)} queries in QUERY_HISTORY")

            # Get version info
            version_info = self._get_snowflake_version()

            # Aggregate metrics per model
            total_execution_time = 0
            total_bytes_scanned = 0
            total_rows_produced = 0
            model_details = []

            for qid, model_info in qid_to_model.items():
                history = history_map.get(qid, {})

                elapsed_time = history.get('TOTAL_ELAPSED_TIME', 0) or 0
                bytes_scanned = history.get('BYTES_SCANNED', 0) or 0
                rows_produced = history.get('ROWS_PRODUCED', 0) or 0

                total_execution_time += elapsed_time
                total_bytes_scanned += bytes_scanned
                total_rows_produced += rows_produced

                model_details.append({
                    'model_name': model_info['model_name'],
                    'fqn': model_info['fqn'],
                    'total_execution_time_ms': elapsed_time,
                    'total_bytes_scanned': bytes_scanned,
                    'total_rows_produced': rows_produced,
                    'query_count': 1,
                    'row_count': model_info['row_count'],
                })

            result = {
                'pipeline_id': pipeline_id,
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'total_execution_time_ms': total_execution_time,
                'total_bytes_scanned': total_bytes_scanned,
                'total_rows_produced': total_rows_produced,
                'model_details': model_details,
                'warehouse': version_info.get('warehouse'),
                'database': version_info.get('database'),
                'schema': version_info.get('schema'),
                'collection_timestamp': datetime.now(timezone.utc).isoformat(),
            }

            logger.info(
                f"Query-time metrics for pipeline {pipeline_id}: "
                f"execution_time={total_execution_time}ms, "
                f"bytes={total_bytes_scanned}, "
                f"rows={total_rows_produced}"
            )

            return result

        except Exception as e:
            logger.error(f"Error collecting metrics by query IDs: {e}", exc_info=True)
            return self._create_empty_result(pipeline_id, datetime.now(timezone.utc).isoformat())

        finally:
            self.sf_conn.close()

    def _create_empty_result(
        self,
        pipeline_id: str,
        start_timestamp: str,
        version_info: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create an empty result structure when no metrics are collected.
        
        Args:
            pipeline_id: Pipeline identifier
            start_timestamp: Pipeline start timestamp
            version_info: Optional Snowflake metadata
            
        Returns:
            Empty metrics dictionary
        """
        if version_info is None:
            version_info = {}
        
        return {
            'pipeline_id': pipeline_id,
            'timestamp': start_timestamp,
            'total_execution_time_ms': 0,
            'total_bytes_scanned': 0,
            'total_rows_produced': 0,
            'model_details': [],
            'warehouse': version_info.get('warehouse'),
            'database': version_info.get('database'),
            'schema': version_info.get('schema'),
            'collection_timestamp': datetime.now(timezone.utc).isoformat(),
        }


def collect_metrics(
    pipeline_id: str,
    executed_models: List[str],
    fqn_models: List[str],
    start_timestamp: str,
    end_timestamp: str,
) -> Dict[str, Any]:
    """
    Collect metrics for a completed pipeline execution.
    
    Main entry point for metrics collection.
    
    Args:
        pipeline_id: Pipeline identifier (A, B, C)
        executed_models: List of executed model names
        fqn_models: List of fully qualified Snowflake names
        start_timestamp: Pipeline start time (ISO format)
        end_timestamp: Pipeline end time (ISO format)
        
    Returns:
        Dictionary containing:
            - pipeline_id: Pipeline identifier
            - timestamp: Pipeline execution start time
            - total_execution_time_ms: Total execution time
            - total_bytes_scanned: Total bytes scanned
            - total_rows_produced: Total rows produced
            - model_details: Per-model breakdown
            - warehouse: Warehouse name
            - database: Database name
            - schema: Schema name
            - collection_timestamp: When metrics were collected
            
    Raises:
        Exception: On connection or query errors
    """
    collector = MetricsCollector()
    return collector.collect_metrics(
        pipeline_id=pipeline_id,
        executed_models=executed_models,
        fqn_models=fqn_models,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )


if __name__ == '__main__':
    # Example usage
    logger.info("Running metrics collector example")
    
    try:
        # Example metrics collection (would be called after pipeline execution)
        example_result = collect_metrics(
            pipeline_id='A',
            executed_models=['model_one', 'model_two'],
            fqn_models=['DBT_DEMO.DEV.MODEL_ONE', 'DBT_DEMO.DEV.MODEL_TWO'],
            start_timestamp=datetime.now(timezone.utc).isoformat(),
            end_timestamp=datetime.now(timezone.utc).isoformat(),
        )
        
        logger.info(f"Metrics result: {example_result}")
    
    except Exception as e:
        logger.error(f"Failed to collect metrics: {e}", exc_info=True)
        sys.exit(1)
