import os
from dataclasses import dataclass


@dataclass
class Config:
    """
    Configuration for the User Anonymization data pipeline.
    
    Attributes:
        total_persons: Total number of persons to fetch from API
        gender: Filter by gender (male/female/empty for all)
        birthday_start: Minimum birth date in YYYY-MM-DD format
        output_path: Path to output database
        faker_api_url: Base URL for the Faker API
        retry_attempts: Number of retry attempts for API requests
        timeout: Request timeout in seconds
    """
    # Data fetching parameters
    total_persons: int = 30000
    gender: str = ""  # Empty for all genders
    birthday_start: str = "1900-01-01"
    
    # Storage parameters
    output_path: str = "./data/anonymization.duckdb"
    report_path: str = "./data/report.json"
    
    # API parameters
    faker_api_url: str = "https://fakerapi.it/api/v2"
    retry_attempts: int = 3
    timeout: int = 30
    
    def __post_init__(self):
        """Apply environment variable overrides if present."""
        # Override from environment variables if present
        if os.environ.get("TOTAL_PERSONS"):
            self.total_persons = int(os.environ.get("TOTAL_PERSONS"))
            
        if os.environ.get("GENDER"):
            self.gender = os.environ.get("GENDER")
            
        if os.environ.get("BIRTHDAY_START"):
            self.birthday_start = os.environ.get("BIRTHDAY_START")
            
        if os.environ.get("OUTPUT_PATH"):
            self.output_path = os.environ.get("OUTPUT_PATH")

        if os.environ.get("REPORT_PATH"):
            self.report_path = os.environ.get("REPORT_PATH")
            
        if os.environ.get("API_URL"):
            self.faker_api_url = os.environ.get("API_URL")
            
        if os.environ.get("RETRY_ATTEMPTS"):
            self.retry_attempts = int(os.environ.get("RETRY_ATTEMPTS"))
            
        if os.environ.get("TIMEOUT"):
            self.timeout = int(os.environ.get("TIMEOUT"))
