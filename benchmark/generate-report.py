#!/usr/bin/env python3
"""
Benchmark validation report generator.

Connects to Postgres or Snowflake databases, extracts data from 10 marts models,
computes validation metrics (row counts and hash checksums), and outputs a
structured JSON report.
"""

import argparse
import hashlib
import json
import logging
import os
import sys
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection as pg_connection
from snowflake.connector import connect as snowflake_connect
from snowflake.connector.connection import SnowflakeConnection


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# Hardcoded list of 10 marts models
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


class DatabaseConnector:
    """Base class for database connectivity."""

    def __init__(self, dialect: str):
        self.dialect = dialect
        self.connection: Optional[Any] = None

    def connect(self) -> None:
        """Connect to the database. Must be implemented by subclasses."""
        raise NotImplementedError

    def disconnect(self) -> None:
        """Disconnect from the database. Must be implemented by subclasses."""
        raise NotImplementedError

    def query(self, sql: str, timeout: int = 300) -> List[Tuple]:
        """Execute query and return results. Must be implemented by subclasses."""
        raise NotImplementedError

    def get_column_names(self, cursor: Any) -> List[str]:
        """Get column names from cursor description."""
        if hasattr(cursor, 'description'):
            return [desc[0] for desc in cursor.description]
        return []


class PostgresConnector(DatabaseConnector):
    """Connector for Postgres database."""

    def __init__(self):
        super().__init__('postgres')
        self.host = 'localhost'
        self.port = 5433
        self.schema = 'public'
        self.database = None

    def connect(self) -> None:
        """Connect to Postgres using environment variables."""
        try:
            user = os.getenv('DBT_PG_USER')
            password = os.getenv('DBT_PG_PASSWORD')
            dbname = os.getenv('DBT_PG_DBNAME')

            if not all([user, password, dbname]):
                raise ValueError(
                    "Missing required Postgres environment variables: "
                    "DBT_PG_USER, DBT_PG_PASSWORD, DBT_PG_DBNAME"
                )

            self.database = dbname
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=user,
                password=password,
                database=dbname,
                connect_timeout=10
            )
            logger.info(f"Connected to Postgres at {self.host}:{self.port}/{dbname}")
        except psycopg2.OperationalError as e:
            logger.error(f"Failed to connect to Postgres: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error connecting to Postgres: {e}")
            raise

    def disconnect(self) -> None:
        """Close Postgres connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Postgres")

    def query(self, sql: str, timeout: int = 300) -> Tuple[List[Tuple], List[str]]:
        """
        Execute query and return results and column names.

        Args:
            sql: SQL query to execute
            timeout: Query timeout in seconds (default 300)

        Returns:
            Tuple of (rows, column_names)
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        try:
            cursor = self.connection.cursor()
            # Set statement timeout for long-running queries
            cursor.execute(f"SET statement_timeout TO {timeout * 1000}")
            cursor.execute(sql)
            rows = cursor.fetchall()
            column_names = self.get_column_names(cursor)
            cursor.close()
            return rows, column_names
        except psycopg2.errors.QueryCanceled:
            logger.error(f"Query timeout after {timeout} seconds")
            raise
        except psycopg2.Error as e:
            logger.error(f"Database query error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during query: {e}")
            raise


class SnowflakeConnector(DatabaseConnector):
    """Connector for Snowflake database."""

    def __init__(self):
        super().__init__('snowflake')
        self.warehouse = 'COMPUTE_WH'
        self.database = 'DBT_DEMO'
        self.schema = 'DEV'

    def connect(self) -> None:
        """Connect to Snowflake using environment variables."""
        try:
            account = os.getenv('SNOWFLAKE_ACCOUNT')
            user = os.getenv('SNOWFLAKE_USER')
            password = os.getenv('SNOWFLAKE_PASSWORD')

            if not all([account, user, password]):
                raise ValueError(
                    "Missing required Snowflake environment variables: "
                    "SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD"
                )

            self.connection = snowflake_connect(
                account=account,
                user=user,
                password=password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                login_timeout=30
            )
            logger.info(
                f"Connected to Snowflake account {account}, "
                f"database {self.database}, schema {self.schema}"
            )
        except Exception as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def disconnect(self) -> None:
        """Close Snowflake connection."""
        if self.connection:
            self.connection.close()
            logger.info("Disconnected from Snowflake")

    def query(self, sql: str, timeout: int = 300) -> Tuple[List[Tuple], List[str]]:
        """
        Execute query and return results and column names.

        Args:
            sql: SQL query to execute
            timeout: Query timeout in seconds (default 300)

        Returns:
            Tuple of (rows, column_names)
        """
        if not self.connection:
            raise RuntimeError("Not connected to database")

        try:
            cursor = self.connection.cursor()
            # Snowflake handles timeouts via query_tag
            cursor.execute(sql)
            rows = cursor.fetchall()
            column_names = [desc[0].lower() for desc in cursor.description]
            cursor.close()
            return rows, column_names
        except Exception as e:
            logger.error(f"Database query error: {e}")
            raise


class DataProcessor:
    """Processes data for validation metrics."""

    # Currency columns that should be rounded to 2 decimals
    CURRENCY_KEYWORDS = {'amount', 'value', 'mv', 'pnl', 'commission'}

    # Rate columns that should be rounded to 6 decimals
    # (reduced from 8 to account for cross-database floating-point precision differences)
    RATE_KEYWORDS = {'rate', 'return', 'pct', 'percentage'}

    @staticmethod
    def is_numeric_type(value: Any) -> bool:
        """Check if value is a numeric type."""
        return isinstance(value, (int, float, Decimal))

    @staticmethod
    def round_value(value: Any, decimals: int) -> Any:
        """
        Round a numeric value to specified decimal places.

        Args:
            value: Value to round
            decimals: Number of decimal places

        Returns:
            Rounded value or original value if not numeric
        """
        if value is None:
            return None

        if not DataProcessor.is_numeric_type(value):
            return value

        try:
            # Convert to float for rounding
            numeric_val = float(value)
            return round(numeric_val, decimals)
        except (ValueError, TypeError):
            return value

    @classmethod
    def get_column_precision(cls, column_name: str) -> Optional[int]:
        """
        Determine rounding precision for a column based on its name.

        Args:
            column_name: Name of the column

        Returns:
            Number of decimal places or None if no rounding needed
        """
        col_lower = column_name.lower()

        # Check for currency columns first (higher priority)
        for keyword in cls.CURRENCY_KEYWORDS:
            if keyword in col_lower:
                return 2

        # Check for rate columns
        for keyword in cls.RATE_KEYWORDS:
            if keyword in col_lower:
                return 6

        # Apply uniform 6-decimal rounding to all numeric columns
        # (reduced from 8 to account for cross-database floating-point precision differences)
        return 6

    @staticmethod
    def normalize_row(
        row: Tuple,
        column_names: List[str],
        precision_map: Dict[str, int]
    ) -> List[str]:
        """
        Normalize a row for hashing: apply rounding and handle NULLs.

        Args:
            row: Tuple of column values
            column_names: List of column names
            precision_map: Map of column names to decimal precision

        Returns:
            List of normalized string values
        """
        normalized = []

        for i, value in enumerate(row):
            col_name = column_names[i] if i < len(column_names) else f"col_{i}"
            precision = precision_map.get(col_name, 8)

            # Handle NULL values
            if value is None:
                normalized.append('')
            else:
                # Apply rounding if numeric
                if DataProcessor.is_numeric_type(value):
                    rounded_value = DataProcessor.round_value(value, precision)
                    normalized.append(str(rounded_value))
                # Normalize dates/timestamps to YYYY-MM-DD format
                elif hasattr(value, 'strftime'):
                    normalized.append(value.strftime('%Y-%m-%d'))
                else:
                    normalized.append(str(value))

        return normalized

    @staticmethod
    def sort_rows(rows: List[Tuple]) -> List[Tuple]:
        """
        Sort rows deterministically for consistent hashing.

        Args:
            rows: List of row tuples

        Returns:
            Sorted list of rows
        """
        try:
            # Sort by all columns, converting None to empty string for comparison
            def sort_key(row: Tuple) -> Tuple:
                return tuple('' if val is None else val for val in row)

            return sorted(rows, key=sort_key)
        except TypeError:
            # If sorting fails due to mixed types, convert all to strings
            def sort_key_str(row: Tuple) -> Tuple:
                return tuple(str(val) if val is not None else '' for val in row)

            return sorted(rows, key=sort_key_str)

    @staticmethod
    def compute_hash(data_string: str, algorithm: str = 'sha256') -> str:
        """
        Compute hash of data string.

        Args:
            data_string: String to hash
            algorithm: Hash algorithm ('md5' or 'sha256', default 'sha256')

        Returns:
            Hex digest of hash
        """
        if algorithm == 'md5':
            return hashlib.md5(data_string.encode()).hexdigest()
        elif algorithm == 'sha256':
            return hashlib.sha256(data_string.encode()).hexdigest()
        else:
            raise ValueError(f"Unsupported hash algorithm: {algorithm}")


class ValidationReportGenerator:
    """Generates validation reports for marts models."""

    def __init__(self, dialect: str, output_path: str = './report.json'):
        """
        Initialize the report generator.

        Args:
            dialect: Database dialect ('postgres' or 'snowflake')
            output_path: Path to save the JSON report
        """
        self.dialect = dialect
        self.output_path = output_path

        # Create appropriate connector
        if dialect.lower() == 'postgres':
            self.connector = PostgresConnector()
        elif dialect.lower() == 'snowflake':
            self.connector = SnowflakeConnector()
        else:
            raise ValueError(f"Unsupported dialect: {dialect}")

        self.data_processor = DataProcessor()
        self.report: Dict[str, Any] = {
            'generated_at': None,
            'dialect': dialect,
            'database': None,
            'schema': None,
            'models': {}
        }

    def generate_report(self) -> None:
        """Generate the complete validation report."""
        try:
            # Connect to database
            logger.info(f"Connecting to {self.dialect}...")
            self.connector.connect()

            # Set metadata
            self.report['generated_at'] = datetime.utcnow().isoformat() + 'Z'
            self.report['database'] = self.connector.database
            self.report['schema'] = self.connector.schema

            # Process each marts model
            logger.info(f"Processing {len(MARTS_MODELS)} marts models...")
            for model_name in MARTS_MODELS:
                logger.info(f"Processing model: {model_name}")
                try:
                    metrics = self._process_model(model_name)
                    self.report['models'][model_name] = metrics
                    logger.info(
                        f"  ✓ {model_name}: {metrics['row_count']} rows, "
                        f"hash={metrics['data_hash'][:16]}..."
                    )
                except Exception as e:
                    logger.error(f"  ✗ Error processing {model_name}: {e}")
                    self.report['models'][model_name] = {
                        'error': str(e),
                        'row_count': None,
                        'data_hash': None
                    }

            # Disconnect and save report
            self.connector.disconnect()
            self._save_report()
            logger.info(f"Report saved to {self.output_path}")

        except Exception as e:
            logger.error(f"Fatal error generating report: {e}")
            raise
        finally:
            if self.connector.connection:
                self.connector.disconnect()

    def _process_model(self, model_name: str) -> Dict[str, Any]:
        """
        Process a single marts model and compute validation metrics.

        Args:
            model_name: Name of the marts model to process

        Returns:
            Dictionary with row_count and data_hash
        """
        # Query data from model
        sql = f"SELECT * FROM {model_name}"
        rows, column_names = self.connector.query(sql)

        # Build precision map for columns
        precision_map = {
            col: self.data_processor.get_column_precision(col)
            for col in column_names
        }

        # Sort rows for deterministic hashing
        sorted_rows = self.data_processor.sort_rows(rows)

        # Normalize and concatenate rows
        normalized_rows = [
            '|'.join(
                self.data_processor.normalize_row(row, column_names, precision_map)
            )
            for row in sorted_rows
        ]

        # Compute hash
        data_string = '\n'.join(normalized_rows)
        data_hash = self.data_processor.compute_hash(data_string, algorithm='sha256')

        return {
            'row_count': len(rows),
            'data_hash': data_hash
        }

    def _save_report(self) -> None:
        """Save the report to JSON file."""
        # Create output directory if it doesn't exist
        output_dir = os.path.dirname(self.output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        with open(self.output_path, 'w') as f:
            json.dump(self.report, f, indent=2)


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description='Generate validation report for benchmark comparison'
    )
    parser.add_argument(
        '--dialect',
        required=True,
        choices=['postgres', 'snowflake'],
        help='Database dialect (postgres or snowflake)'
    )
    parser.add_argument(
        '--output',
        default='./report.json',
        help='Path to save the JSON report (default: ./report.json)'
    )

    args = parser.parse_args()

    try:
        generator = ValidationReportGenerator(args.dialect, args.output)
        generator.generate_report()
        logger.info("Report generation completed successfully")
        return 0
    except KeyboardInterrupt:
        logger.info("Report generation cancelled by user")
        return 130
    except Exception as e:
        logger.error(f"Report generation failed: {e}")
        return 1


if __name__ == '__main__':
    sys.exit(main())
