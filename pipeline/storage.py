import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

import duckdb

from .schema import PERSON_SCHEMA, REPORTING_VIEWS

logger = logging.getLogger(__name__)


class DuckDBStorage:
    """
    DuckDB-based storage for anonymized person data.
    
    Features:
    - Fast columnar storage with SQL query capabilities
    - Parquet export/import for data sharing
    - Schema validation
    - Basic data quality checks
    - Predefined views for reporting
    """
    
    def __init__(self, database_path: str = ":memory:"):
        """
        Initialize DuckDB storage.
        
        Args:
            database_path: Path to DuckDB database file or ":memory:" for in-memory database
        """
        self.database_path = database_path
        
        # Create database directory if it doesn't exist
        if database_path != ":memory:":
            db_dir = Path(database_path).parent
            db_dir.mkdir(parents=True, exist_ok=True)
        
        # Connect to DuckDB
        self.conn = duckdb.connect(database=database_path)
        logger.debug(f"Connected to DuckDB at {database_path}")
    
    def create_schema(self):
        """Create the database schema."""
        # Use the schema definition from schema.py
        create_table_sql = PERSON_SCHEMA.get_create_table_sql()
        self.conn.execute(create_table_sql)
        logger.debug(f"Created table schema for {PERSON_SCHEMA.name}")
    
    def create_views(self):
        """Create database views for reporting purposes."""
        logger.debug("Creating database views for reporting")
        
        for view in REPORTING_VIEWS:
            create_view_sql = view.get_create_view_sql(PERSON_SCHEMA.name)
            try:
                self.conn.execute(create_view_sql)
                logger.debug(f"Created view: {view.name}")
            except Exception as e:
                logger.error(f"Error creating view {view.name}: {str(e)}")
        
        logger.debug(f"Created {len(REPORTING_VIEWS)} database views successfully")
    
    def list_views(self):
        """List all views in the database."""
        return self.execute_query("SHOW VIEWS")
    
    def get_view_data(self, view_name: str, limit: int = 10):
        """
        Retrieve data from a specific view.
        
        Args:
            view_name: Name of the view to query
            limit: Maximum number of rows to return
            
        Returns:
            List of result rows as dictionaries
        """
        try:
            query = f"SELECT * FROM {view_name} LIMIT {limit}"
            return self.execute_query(query)
        except Exception as e:
            logger.error(f"Error retrieving data from view {view_name}: {str(e)}")
            return []
    
    def store_persons(self, persons: List[Dict[str, Any]]) -> int:
        """
        Store anonymized person records in the database.
        
        Args:
            persons: List of anonymized person dictionaries
            
        Returns:
            Number of records stored
        """
        if not persons:
            logger.warning("No persons to store")
            return 0
        
        # Insert data in batches
        batch_size = 5000
        total_stored = 0
        
        for i in range(0, len(persons), batch_size):
            batch = persons[i:i + batch_size]
            try:
                # Convert batch to Pandas DataFrame and insert directly
                # This is more efficient than using a temporary table
                import pandas as pd
                df = pd.DataFrame(batch)
                
                # Insert the DataFrame directly into the table
                self.conn.execute(f"INSERT INTO {PERSON_SCHEMA.name} SELECT * FROM df")
                
                total_stored += len(batch)
                logger.debug(f"Stored batch of {len(batch)} persons (total: {total_stored})")
                
            except Exception as e:
                logger.error(f"Error storing batch: {str(e)}")
                # Continue with next batch
                continue
        
        logger.debug(f"Total persons stored: {total_stored}")
        if total_stored != len(persons):
            logger.error(f"Mismatch in stored records: {total_stored} out of {len(persons)}")
            raise RuntimeError("Mismatch in stored records")
        return total_stored
    
    def export_to_parquet(self, output_path: str) -> bool:
        """
        Export the database to Parquet format.
        
        Args:
            output_path: Path to output Parquet file
            
        Returns:
            True if export was successful, False otherwise
        """
        try:
            # Create directory if it doesn't exist
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Export data to Parquet
            self.conn.execute(f"""
                COPY (SELECT * FROM {PERSON_SCHEMA.name}) TO '{output_path}' (FORMAT PARQUET)
            """)
            
            logger.info(f"Exported data to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error exporting to Parquet: {str(e)}")
            return False
    
    def import_from_parquet(self, input_path: str) -> int:
        """
        Import data from Parquet format.
        
        Args:
            input_path: Path to input Parquet file
            
        Returns:
            Number of records imported
        """
        try:
            # Create schema if it doesn't exist
            self.create_schema()
            
            # Import data from Parquet
            self.conn.execute(f"""
                INSERT INTO {PERSON_SCHEMA.name}
                SELECT * FROM read_parquet('{input_path}')
            """)
            
            # Count imported records
            result = self.conn.execute(f"SELECT COUNT(*) FROM {PERSON_SCHEMA.name}").fetchone()
            count = result[0] if result else 0
            
            logger.info(f"Imported {count} records from {input_path}")
            return count
            
        except Exception as e:
            logger.error(f"Error importing from Parquet: {str(e)}")
            return 0
    
    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query: SQL query to execute
            parameters: Query parameters
            
        Returns:
            List of result rows as dictionaries
        """
        try:
            # Execute query
            result = self.conn.execute(query, parameters if parameters else {})
            
            # Convert result to list of dictionaries
            columns = [col[0] for col in result.description]
            rows = result.fetchall()
            
            return [dict(zip(columns, row)) for row in rows]
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Parameters: {parameters}")
            return []
    
    def close(self):
        """Close the database connection."""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        """Enable use as a context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close connection when exiting context."""
        self.close()
