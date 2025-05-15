import os
import json
from pathlib import Path
from unittest.mock import patch, MagicMock, mock_open

import pytest
import pandas as pd
import duckdb

from pipeline.storage import DuckDBStorage
from pipeline.schema import PERSON_SCHEMA, REPORTING_VIEWS


class TestDuckDBStorage:
    """Tests for the DuckDBStorage class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Use in-memory database for testing
        self.storage = DuckDBStorage(":memory:")
        
        # Sample person data for testing
        self.sample_persons = [
            {
                "gender": "male",
                "country": "United States",
                "city": "New York",
                "country_code": "US",
                "email": "gmail.com",
                "birthday": "[30-40]",
                "latitude": 40.7128,
                "longitude": -74.0060,
                "firstname": "****",
                "lastname": "****",
                "phone": "****",
                "street": "****",
                "streetName": "****",
                "buildingNumber": "****",
                "zipcode": "****",
                "image": "****",
                "website": "****"
            },
            {
                "gender": "female",
                "country": "Germany",
                "city": "Berlin",
                "country_code": "DE",
                "email": "gmail.com",
                "birthday": "[20-30]",
                "latitude": 52.5200,
                "longitude": 13.4050,
                "firstname": "****",
                "lastname": "****",
                "phone": "****",
                "street": "****",
                "streetName": "****",
                "buildingNumber": "****",
                "zipcode": "****",
                "image": "****",
                "website": "****"
            },
            {
                "gender": "male",
                "country": "Germany",
                "city": "Munich",
                "country_code": "DE",
                "email": "yahoo.com",
                "birthday": "[40-50]",
                "latitude": 48.1351,
                "longitude": 11.5820,
                "firstname": "****",
                "lastname": "****",
                "phone": "****",
                "street": "****",
                "streetName": "****",
                "buildingNumber": "****",
                "zipcode": "****",
                "image": "****",
                "website": "****"
            },
            {
                "gender": "female",
                "country": "Japan",
                "city": "Tokyo",
                "country_code": "JP",
                "email": "gmail.com",
                "birthday": "[20-30]",
                "latitude": 35.6762,
                "longitude": 139.6503,
                "firstname": "****",
                "lastname": "****",
                "phone": "****",
                "street": "****",
                "streetName": "****",
                "buildingNumber": "****",
                "zipcode": "****",
                "image": "****",
                "website": "****"
            },
            {
                "gender": "male",
                "country": "United States",
                "city": "San Francisco",
                "country_code": "US",
                "email": "hotmail.com",
                "birthday": "[50-60]",
                "latitude": 37.7749,
                "longitude": -122.4194,
                "firstname": "****",
                "lastname": "****",
                "phone": "****",
                "street": "****",
                "streetName": "****",
                "buildingNumber": "****",
                "zipcode": "****",
                "image": "****",
                "website": "****"
            }
        ]
    
    def teardown_method(self):
        """Tear down test fixtures."""
        self.storage.close()
    
    def test_create_schema(self):
        """Test creating the database schema."""
        # Create the schema
        self.storage.create_schema()
        
        # Check that the table exists
        result = self.storage.execute_query(f"SELECT * FROM information_schema.tables WHERE table_name = '{PERSON_SCHEMA.name}'")
        assert len(result) == 1
        assert result[0]["table_name"] == PERSON_SCHEMA.name
        
        # Check that all columns are created
        result = self.storage.execute_query(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{PERSON_SCHEMA.name}'")
        column_names = [row["column_name"] for row in result]
        
        for field in PERSON_SCHEMA.fields:
            assert field.name in column_names
    
    def test_store_persons(self):
        """Test storing person records."""
        # Create the schema
        self.storage.create_schema()
        
        # Store the sample persons
        count = self.storage.store_persons(self.sample_persons)
        
        # Check the result
        assert count == len(self.sample_persons)
        
        # Verify the data in the database
        result = self.storage.execute_query(f"SELECT * FROM {PERSON_SCHEMA.name}")
        assert len(result) == len(self.sample_persons)
        
        # Check specific data points
        countries = [row["country"] for row in result]
        assert "Germany" in countries
        assert "United States" in countries
        assert "Japan" in countries
        
        # Check email domains
        emails = [row["email"] for row in result]
        assert "gmail.com" in emails
        assert "yahoo.com" in emails
        assert "hotmail.com" in emails
    
    def test_store_empty_persons(self):
        """Test storing empty list of persons."""
        # Create the schema
        self.storage.create_schema()
        
        # Store empty list
        count = self.storage.store_persons([])
        
        # Check the result
        assert count == 0
    
    def test_create_views(self):
        """Test creating views for reporting."""
        # Create the schema and store data
        self.storage.create_schema()
        self.storage.store_persons(self.sample_persons)
        
        # Create the views
        self.storage.create_views()
        
        # Check that all views are created
        for view in REPORTING_VIEWS:
            result = self.storage.execute_query(f"SELECT * FROM information_schema.views WHERE table_name = '{view.name}'")
            assert len(result) == 1
            assert result[0]["table_name"] == view.name
    
    def test_list_views(self):
        """Test listing all views."""
        # Create the schema and views
        self.storage.create_schema()
        self.storage.store_persons(self.sample_persons)
        self.storage.create_views()
        
        # List the views
        views = self.storage.list_views()
        
        # Check the result
        view_names = [row["view_name"] for row in views]
        for view in REPORTING_VIEWS:
            assert view.name in view_names
    
    def test_get_view_data(self):
        """Test retrieving data from a view."""
        # Create the schema, store data, and create views
        self.storage.create_schema()
        self.storage.store_persons(self.sample_persons)
        self.storage.create_views()
        
        # Get data from the country_stats view
        result = self.storage.get_view_data("country_stats")
        
        # Check the result
        assert len(result) > 0
        assert "country" in result[0]
        assert "user_count" in result[0]
        assert "percentage" in result[0]
        
        # Check specific countries
        countries = [row["country"] for row in result]
        assert "Germany" in countries
        assert "United States" in countries
        assert "Japan" in countries
    
    def test_get_view_data_nonexistent(self):
        """Test retrieving data from a nonexistent view."""
        # Create the schema
        self.storage.create_schema()
        
        # Try to get data from a nonexistent view
        result = self.storage.get_view_data("nonexistent_view")
        
        # Check the result is an empty list
        assert result == []
    
    def test_execute_query(self):
        """Test executing a SQL query."""
        # Create the schema and store data
        self.storage.create_schema()
        self.storage.store_persons(self.sample_persons)
        
        # Execute a simple query
        result = self.storage.execute_query(f"SELECT DISTINCT country FROM {PERSON_SCHEMA.name} ORDER BY country")
        
        # Check the result
        assert len(result) == 3
        assert result[0]["country"] == "Germany"
        assert result[1]["country"] == "Japan"
        assert result[2]["country"] == "United States"
    
    def test_execute_query_with_parameters(self):
        """Test executing a SQL query with parameters."""
        # Create the schema and store data
        self.storage.create_schema()
        self.storage.store_persons(self.sample_persons)
        
        # Execute a query with parameters
        result = self.storage.execute_query(
            f"SELECT * FROM {PERSON_SCHEMA.name} WHERE country = $country",
            {"country": "Germany"}
        )
        
        # Check the result
        assert len(result) == 2
        assert all(row["country"] == "Germany" for row in result)
        
        # One should be from Berlin, one from Munich
        cities = [row["city"] for row in result]
        assert "Berlin" in cities
        assert "Munich" in cities
    
    def test_execute_query_error(self):
        """Test handling errors in query execution."""
        # Create the schema
        self.storage.create_schema()
        
        # Execute an invalid query
        result = self.storage.execute_query("SELECT * FROM nonexistent_table")
        
        # Check the result is an empty list
        assert result == []
    
    @pytest.mark.skipif(os.name == 'nt', reason="File paths are different on Windows")
    def test_export_to_parquet(self, tmp_path):
        """Test exporting data to Parquet format."""
        # Create a temporary file path
        output_path = str(tmp_path / "test_export.parquet")
        
        # Create the schema and store data
        self.storage.create_schema()
        self.storage.store_persons(self.sample_persons)
        
        # Export to Parquet
        result = self.storage.export_to_parquet(output_path)
        
        # Check the result
        assert result is True
        assert os.path.exists(output_path)
        
        # Verify the exported data
        imported_df = pd.read_parquet(output_path)
        assert len(imported_df) == len(self.sample_persons)
    
    @pytest.mark.skipif(os.name == 'nt', reason="File paths are different on Windows")
    def test_import_from_parquet(self, tmp_path):
        """Test importing data from Parquet format."""
        # First export data to a temporary file
        export_path = str(tmp_path / "test_export.parquet")
        
        # Create the schema and store data
        self.storage.create_schema()
        self.storage.store_persons(self.sample_persons)
        self.storage.export_to_parquet(export_path)
        
        # Create a new storage instance
        new_storage = DuckDBStorage(":memory:")
        
        # Import from Parquet
        count = new_storage.import_from_parquet(export_path)
        
        # Check the result
        assert count == len(self.sample_persons)
        
        # Verify the imported data
        result = new_storage.execute_query(f"SELECT * FROM {PERSON_SCHEMA.name}")
        assert len(result) == len(self.sample_persons)
        
        # Clean up
        new_storage.close()
    
    def test_context_manager(self):
        """Test using the storage as a context manager."""
        with patch('duckdb.connect') as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            
            with DuckDBStorage(":memory:") as storage:
                assert isinstance(storage, DuckDBStorage)
            
            # Verify the connection was closed
            mock_conn.close.assert_called_once()
    
    def test_export_error_handling(self, tmp_path):
        """Test error handling during export."""
        # Create a path that will cause an error (a directory instead of a file)
        invalid_dir = tmp_path / "invalid_dir"
        invalid_dir.mkdir()
        output_path = str(invalid_dir)  # Using directory as a file will cause an error
        
        # Create the schema
        self.storage.create_schema()
        
        # Try to export to an invalid location
        result = self.storage.export_to_parquet(output_path)
        
        # Check the result
        assert result is False
    
    def test_import_error_handling(self, tmp_path):
        """Test error handling during import."""
        # Create a nonexistent file path
        input_path = str(tmp_path / "nonexistent.parquet")
        
        # Try to import from a nonexistent file
        count = self.storage.import_from_parquet(input_path)
        
        # Check the result
        assert count == 0
    
    def test_store_persons_batch_error(self):
        """Test error handling during batch storage."""
        # Create the schema
        self.storage.create_schema()
        
        # Create a sample with invalid records that will cause errors
        large_sample = self.sample_persons.copy()
        # Add an invalid record that will cause a schema validation error
        invalid_record = {
            "gender": "other",
            "country": "France",
            # Missing required fields that will cause an error during insert
        }
        large_sample.append(invalid_record)
        
        # Mock the pandas DataFrame to simulate an error during data insertion
        with patch('pandas.DataFrame', side_effect=Exception("Test DataFrame error")):
            # This should now raise a RuntimeError
            with pytest.raises(RuntimeError, match="Failed to store persons"):
                self.storage.store_persons(large_sample)
