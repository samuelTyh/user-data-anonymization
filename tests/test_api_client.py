import pytest
import requests
from unittest.mock import patch, MagicMock

from pipeline.api_client import FakerAPIClient


class TestFakerAPIClient:
    """Tests for the FakerAPIClient class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.api_client = FakerAPIClient(
            base_url="https://fakerapi.it/api/v2", 
            retry_attempts=1,
            backoff_factor=0.1,
            timeout=1
        )
        
        # Sample API response
        self.mock_response = {
            "status": "OK",
            "code": 200,
            "total": 3,
            "data": [
                {
                    "id": 1,
                    "firstname": "Test",
                    "lastname": "User",
                    "email": "test@example.com",
                    "phone": "123456789",
                    "birthday": "1990-01-01",
                    "gender": "male",
                    "address": {
                        "id": 1,
                        "street": "123 Test St",
                        "streetName": "Test Street",
                        "buildingNumber": "123",
                        "city": "Test City",
                        "zipcode": "12345",
                        "country": "Test Country",
                        "county_code": "TC",
                        "latitude": 0.0,
                        "longitude": 0.0
                    },
                    "website": "http://example.com",
                    "image": "http://example.com/image.jpg"
                },
                {
                    "id": 2,
                    "firstname": "Jane",
                    "lastname": "Doe",
                    "email": "jane@example.com",
                    "phone": "987654321",
                    "birthday": "1991-02-02",
                    "gender": "female",
                    "address": {
                        "id": 2,
                        "street": "456 Test St",
                        "streetName": "Test Avenue",
                        "buildingNumber": "456",
                        "city": "Another City",
                        "zipcode": "54321",
                        "country": "Test Country",
                        "county_code": "TC",
                        "latitude": 1.0,
                        "longitude": 1.0
                    },
                    "website": "http://example.com/jane",
                    "image": "http://example.com/jane.jpg"
                },
                {
                    "id": 3,
                    "firstname": "John",
                    "lastname": "Smith",
                    "email": "john@example.com",
                    "phone": "555555555",
                    "birthday": "1992-03-03",
                    "gender": "male",
                    "address": {
                        "id": 3,
                        "street": "789 Test St",
                        "streetName": "Test Road",
                        "buildingNumber": "789",
                        "city": "Third City",
                        "zipcode": "67890",
                        "country": "Another Country",
                        "county_code": "AC",
                        "latitude": 2.0,
                        "longitude": 2.0
                    },
                    "website": "http://example.com/john",
                    "image": "http://example.com/john.jpg"
                }
            ]
        }

    def teardown_method(self):
        """Tear down test fixtures."""
        self.api_client.close()
    
    @patch('requests.Session.get')
    def test_get_persons_success(self, mock_get):
        """Test successful API call to get_persons."""
        # Configure the mock to return a successful response
        mock_response = MagicMock()
        mock_response.json.return_value = self.mock_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the method under test
        persons = self.api_client.get_persons(quantity=3)
        
        # Verify the result
        assert len(persons) == 3
        assert persons[0]["firstname"] == "Test"
        assert persons[1]["firstname"] == "Jane"
        assert persons[2]["firstname"] == "John"
        
        # Verify the API was called with the correct parameters
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert kwargs["params"]["_quantity"] == 3
        assert "_birthday_start" in kwargs["params"]
        assert kwargs["timeout"] == 1

    @patch('requests.Session.get')
    def test_get_persons_with_gender_filter(self, mock_get):
        """Test filtering by gender in get_persons."""
        # Configure the mock to return filtered data
        filtered_response = self.mock_response.copy()
        filtered_response["data"] = [person for person in self.mock_response["data"] if person["gender"] == "female"]
        filtered_response["total"] = len(filtered_response["data"])
        
        mock_response = MagicMock()
        mock_response.json.return_value = filtered_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the method under test with gender filter
        persons = self.api_client.get_persons(quantity=3, gender="female")
        
        # Verify the result
        assert len(persons) == 1
        assert persons[0]["firstname"] == "Jane"
        assert persons[0]["gender"] == "female"
        
        # Verify the API was called with the correct parameters
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert kwargs["params"]["_quantity"] == 3
        assert kwargs["params"]["_gender"] == "female"

    @patch('requests.Session.get')
    def test_get_persons_api_error(self, mock_get):
        """Test handling of API error in get_persons."""
        # Configure the mock to return an error response
        error_response = {
            "status": "ERROR",
            "code": 400,
            "message": "Invalid parameters"
        }
        
        mock_response = MagicMock()
        mock_response.json.return_value = error_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the method under test and expect an exception
        with pytest.raises(ValueError, match="API error"):
            self.api_client.get_persons()

    @patch('requests.Session.get')
    def test_get_persons_network_error(self, mock_get):
        """Test handling of network error in get_persons."""
        # Configure the mock to raise a requests exception
        mock_get.side_effect = requests.exceptions.RequestException("Network error")
        
        # Call the method under test and expect an exception
        with pytest.raises(requests.exceptions.RequestException):
            self.api_client.get_persons()

    @patch('requests.Session.get')
    def test_get_persons_timeout(self, mock_get):
        """Test handling of timeout in get_persons."""
        # Configure the mock to raise a timeout exception
        mock_get.side_effect = requests.exceptions.Timeout("Request timed out")
        
        # Call the method under test and expect an exception
        with pytest.raises(requests.exceptions.Timeout):
            self.api_client.get_persons()
    
    def test_context_manager(self):
        """Test using the API client as a context manager."""
        with patch('requests.Session.close') as mock_close:
            with FakerAPIClient() as client:
                assert isinstance(client, FakerAPIClient)
            
            # Verify the session was closed
            mock_close.assert_called_once()

    @patch('requests.Session.get')
    def test_large_quantity_warning(self, mock_get):
        """Test warning for large quantity request."""
        # Configure the mock to return a successful response
        mock_response = MagicMock()
        mock_response.json.return_value = self.mock_response
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Call the method with a large quantity
        with patch('pipeline.api_client.logger.warning') as mock_warning:
            self.api_client.get_persons(quantity=1500)
            
            # Verify the warning was logged
            mock_warning.assert_called_once_with("API limit quantity to 1000 per request")
            
        # Verify the API was called with the maximum quantity
        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        assert kwargs["params"]["_quantity"] == 1000
