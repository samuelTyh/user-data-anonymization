import logging
import time
from typing import Dict, List, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger(__name__)


class FakerAPIClient:
    """
    Client for interacting with the Faker API.
    
    Implements best practices for API interaction:
    - Connection pooling
    - Retry mechanism with exponential backoff
    - Timeout handling
    - Error handling and logging
    """
    
    def __init__(
        self, 
        base_url: str = "https://fakerapi.it/api/v2",
        retry_attempts: int = 3,
        backoff_factor: float = 0.5,
        timeout: int = 30
    ):
        """
        Initialize the Faker API client.
        
        Args:
            base_url: Base URL for the Faker API
            retry_attempts: Number of retry attempts for failed requests
            backoff_factor: Backoff factor for retries
            timeout: Request timeout in seconds
        """
        self.base_url = base_url
        self.timeout = timeout
        
        # Create a session for connection pooling
        self.session = requests.Session()
        
        # Set up retry strategy
        retry_strategy = Retry(
            total=retry_attempts,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
    
    def get_persons(
        self, 
        quantity: int = 10, 
        gender: str = "", 
        birthday_start: str = "1900-01-01",
    ) -> List[Dict[str, Any]]:
        """
        Fetch person data from the Faker API.
        
        Args:
            quantity: Number of persons to fetch (max 1000 per request)
            gender: Filter by gender (male/female/empty for all)
            birthday_start: Minimum birth date in YYYY-MM-DD format
            
        Returns:
            List of person data dictionaries
        
        Raises:
            ValueError: If the API returns an error
            requests.RequestException: If there's a network issue
        """
        if quantity > 1000:
            logger.warning("API limit quantity to 1000 per request")
        
        # Build query parameters
        params = {
            "_quantity": min(quantity, 1000),
            "_birthday_start": birthday_start
        }
        
        # Add gender filter if specified
        if gender:
            params["_gender"] = gender
        
        url = f"{self.base_url}/persons"
        
        try:
            start_time = time.time()
            response = self.session.get(
                url, 
                params=params, 
                timeout=self.timeout
            )
            elapsed_time = time.time() - start_time
            
            logger.debug(f"Request completed in {elapsed_time:.2f}s")
            
            # Raise an exception for HTTP errors
            response.raise_for_status()
            
            data = response.json()
            
            # Check API response status
            if data.get("status") != "OK":
                error_msg = data.get("message", "Unknown API error")
                logger.error(f"API error: {error_msg}")
                raise ValueError(f"API error: {error_msg}")
            
            # Perform basic data validation
            persons = data.get("data", [])
            
            if not persons:
                logger.warning("API returned empty data")
            
            logger.debug(f"Successfully retrieved {len(persons)} persons")
            return persons
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out after {self.timeout}s")
            raise
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {str(e)}")
            raise
    
    def close(self):
        """Close the session."""
        self.session.close()
    
    def __enter__(self):
        """Enable use as a context manager."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close session when exiting context."""
        self.close()
