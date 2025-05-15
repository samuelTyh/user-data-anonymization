from unittest.mock import patch, MagicMock, mock_open

from pipeline.reporter import ReportGenerator
from pipeline.storage import DuckDBStorage


class TestReportGenerator:
    """Tests for the ReportGenerator class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Mock DuckDBStorage
        self.mock_storage = MagicMock(spec=DuckDBStorage)
        
        # Create ReportGenerator with mock storage
        self.reporter = ReportGenerator(self.mock_storage)
        
        # Sample report data
        self.sample_email_provider_stats = [
            {"email_provider": "gmail.com", "user_count": 150, "percentage": 50.0},
            {"email_provider": "yahoo.com", "user_count": 90, "percentage": 30.0},
            {"email_provider": "hotmail.com", "user_count": 60, "percentage": 20.0}
        ]
        
        self.sample_country_stats = [
            {"country": "United States", "user_count": 120, "percentage": 40.0},
            {"country": "Germany", "user_count": 90, "percentage": 30.0},
            {"country": "Japan", "user_count": 45, "percentage": 15.0},
            {"country": "United Kingdom", "user_count": 30, "percentage": 10.0},
            {"country": "Canada", "user_count": 15, "percentage": 5.0}
        ]
        
        self.sample_age_group_stats = [
            {"age_group": "[20-30]", "user_count": 75, "percentage": 25.0},
            {"age_group": "[30-40]", "user_count": 120, "percentage": 40.0},
            {"age_group": "[40-50]", "user_count": 60, "percentage": 20.0},
            {"age_group": "[50-60]", "user_count": 30, "percentage": 10.0},
            {"age_group": "[60-70]", "user_count": 15, "percentage": 5.0}
        ]
        
        self.sample_email_by_country = [
            {"country": "Germany", "email_provider": "gmail.com", "user_count": 45, "country_percentage": 50.0},
            {"country": "Germany", "email_provider": "yahoo.com", "user_count": 27, "country_percentage": 30.0},
            {"country": "Germany", "email_provider": "hotmail.com", "user_count": 18, "country_percentage": 20.0},
            {"country": "United States", "email_provider": "gmail.com", "user_count": 60, "country_percentage": 50.0},
            {"country": "United States", "email_provider": "yahoo.com", "user_count": 36, "country_percentage": 30.0},
            {"country": "United States", "email_provider": "hotmail.com", "user_count": 24, "country_percentage": 20.0}
        ]
    
    def test_get_germany_gmail_percentage(self):
        """Test retrieving Gmail usage percentage in Germany."""
        # Mock execute_query to return Germany Gmail percentage
        self.mock_storage.execute_query.return_value = [{"percentage": 50.0}]
        
        # Call the method under test
        percentage = self.reporter.get_germany_gmail_percentage()
        
        # Verify the result
        assert percentage == 50.0
        
        # Verify the query execution
        self.mock_storage.execute_query.assert_called_once()
        query = self.mock_storage.execute_query.call_args[0][0]
        assert "Germany" in query
        assert "gmail.com" in query
    
    def test_get_germany_gmail_percentage_empty_result(self):
        """Test handling empty result for Germany Gmail percentage."""
        # Mock execute_query to return empty result
        self.mock_storage.execute_query.return_value = []
        
        # Call the method under test
        percentage = self.reporter.get_germany_gmail_percentage()
        
        # Verify the result is the default value
        assert percentage == 0.0
        
        # Verify the query execution
        self.mock_storage.execute_query.assert_called_once()
    
    def test_get_top_gmail_countries(self):
        """Test retrieving top countries using Gmail."""
        # Mock execute_query to return top Gmail countries
        mock_result = [
            {"rank": 1, "country": "United States", "user_count": 60},
            {"rank": 2, "country": "Germany", "user_count": 45},
            {"rank": 3, "country": "Japan", "user_count": 30}
        ]
        self.mock_storage.execute_query.return_value = mock_result
        
        # Call the method under test
        result = self.reporter.get_top_gmail_countries(limit=3)
        
        # Verify the result
        assert result == mock_result
        assert len(result) == 3
        assert result[0]["country"] == "United States"
        assert result[1]["country"] == "Germany"
        assert result[2]["country"] == "Japan"
        
        # Verify the query execution
        self.mock_storage.execute_query.assert_called_once()
        query, params = self.mock_storage.execute_query.call_args[0]
        assert "gmail.com" in query
        assert params["limit"] == 3
    
    def test_get_top_gmail_countries_empty_result(self):
        """Test handling empty result for top Gmail countries."""
        # Mock execute_query to return empty result
        self.mock_storage.execute_query.return_value = []
        
        # Call the method under test
        result = self.reporter.get_top_gmail_countries(limit=3)
        
        # Verify the result is an empty list
        assert result == []
        
        # Verify the query execution
        self.mock_storage.execute_query.assert_called_once()
    
    def test_get_seniors_with_gmail(self):
        """Test retrieving seniors using Gmail."""
        # Mock execute_query to return seniors count
        self.mock_storage.execute_query.return_value = [{"senior_count": 15}]
        
        # Call the method under test
        count = self.reporter.get_seniors_with_gmail(age_threshold=60)
        
        # Verify the result
        assert count == 15
        
        # Verify the query execution
        self.mock_storage.execute_query.assert_called_once()
        query = self.mock_storage.execute_query.call_args[0][0]
        assert "gmail.com" in query
        assert "[60-" in query  # Age group pattern
    
    def test_get_seniors_with_gmail_empty_result(self):
        """Test handling empty result for seniors with Gmail."""
        # Mock execute_query to return empty result
        self.mock_storage.execute_query.return_value = []
        
        # Call the method under test
        count = self.reporter.get_seniors_with_gmail(age_threshold=60)
        
        # Verify the result is the default value
        assert count == 0
        
        # Verify the query execution
        self.mock_storage.execute_query.assert_called_once()
    
    def test_generate_full_report(self):
        """Test generating a complete report."""
        # Set up mock returns for each method
        # Mock get_germany_gmail_percentage
        self.mock_storage.execute_query.return_value = [{"percentage": 50.0}]
        
        # Mock get_top_gmail_countries
        top_countries = [
            {"rank": 1, "country": "United States", "user_count": 60},
            {"rank": 2, "country": "Germany", "user_count": 45},
            {"rank": 3, "country": "Japan", "user_count": 30}
        ]
        
        # Mock get_seniors_with_gmail
        seniors_count = 15
        
        # Mock get_view_data for various views
        self.mock_storage.get_view_data.side_effect = [
            self.sample_email_provider_stats[:5],  # email_provider_stats
            self.sample_country_stats[:5],         # country_stats
            self.sample_age_group_stats            # age_group_stats
        ]
        
        # Set up patched methods on the ReportGenerator instance
        with patch.object(self.reporter, 'get_germany_gmail_percentage', return_value=50.0) as mock_gmail_pct, \
             patch.object(self.reporter, 'get_top_gmail_countries', return_value=top_countries) as mock_top_countries, \
             patch.object(self.reporter, 'get_seniors_with_gmail', return_value=seniors_count) as mock_seniors:
            
            # Call the method under test
            report = self.reporter.generate_full_report()
            
            # Verify the report structure and content
            assert "germany_gmail_percentage" in report
            assert report["germany_gmail_percentage"] == 50.0
            
            assert "top_gmail_countries" in report
            assert report["top_gmail_countries"] == top_countries
            
            assert "seniors_with_gmail" in report
            assert report["seniors_with_gmail"] == seniors_count
            
            assert "email_provider_stats" in report
            assert "country_stats" in report
            assert "age_group_stats" in report
            
            # Verify method calls
            mock_gmail_pct.assert_called_once()
            mock_top_countries.assert_called_once_with(limit=3)
            mock_seniors.assert_called_once_with(age_threshold=60)
            
            # Verify get_view_data calls
            assert self.mock_storage.get_view_data.call_count == 3
    
    def test_save_report_to_json(self):
        """Test saving the report to a JSON file."""
        # Mock generate_full_report to return a sample report
        sample_report = {
            "germany_gmail_percentage": 50.0,
            "top_gmail_countries": [
                {"rank": 1, "country": "United States", "user_count": 60},
                {"rank": 2, "country": "Germany", "user_count": 45},
                {"rank": 3, "country": "Japan", "user_count": 30}
            ],
            "seniors_with_gmail": 15,
            "email_provider_stats": self.sample_email_provider_stats[:5],
            "country_stats": self.sample_country_stats[:5],
            "age_group_stats": self.sample_age_group_stats
        }
        
        # Set up patched methods
        with patch.object(self.reporter, 'generate_full_report', return_value=sample_report) as mock_generate, \
             patch('builtins.open', mock_open()) as mock_file, \
             patch('json.dump') as mock_json_dump, \
             patch('pathlib.Path.mkdir') as mock_mkdir:
            
            # Call the method under test
            result = self.reporter.save_report_to_json("./data/report.json")
            
            # Verify the result
            assert result is True
            
            # Verify method calls
            mock_generate.assert_called_once()
            mock_file.assert_called_once_with("./data/report.json", 'w')
            mock_json_dump.assert_called_once()
            
            # Verify that the directory was created
            mock_mkdir.assert_called_once_with(parents=True, exist_ok=True)
    
    def test_save_report_to_json_error(self):
        """Test handling errors when saving the report to a JSON file."""
        # Mock generate_full_report to raise an exception
        with patch.object(self.reporter, 'generate_full_report', side_effect=Exception("Test error")) as mock_generate:
            
            # Call the method under test
            result = self.reporter.save_report_to_json("./data/report.json")
            
            # Verify the result
            assert result is False
            
            # Verify method calls
            mock_generate.assert_called_once()
