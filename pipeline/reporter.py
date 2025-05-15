import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Any

from .storage import DuckDBStorage
from .schema import PERSON_SCHEMA

logger = logging.getLogger(__name__)


class ReportGenerator:
    """
    Generates analytical reports from anonymized person data.
    
    Features:
    - SQL-based reporting (one query per report)
    - Percentage calculations
    - Ranking functions
    - Age-based filtering
    """
    
    def __init__(self, storage: DuckDBStorage):
        """
        Initialize the report generator.
        
        Args:
            storage: DuckDB storage instance containing person data
        """
        self.storage = storage
        self.table_name = PERSON_SCHEMA.name
    
    def get_germany_gmail_percentage(self) -> float:
        """
        Calculate the percentage of users in Germany using Gmail.
        
        Returns:
            Percentage as a float
        """
        query = """
        SELECT country_percentage AS percentage
        FROM email_by_country
        WHERE country = 'Germany' AND email_provider = 'gmail.com'
        """
        
        result = self.storage.execute_query(query)
        
        if result and 'percentage' in result[0]:
            percentage = result[0]['percentage']
            logger.debug(f"Percentage of users in Germany using Gmail: {percentage:.2f}%")
            return percentage
        else:
            logger.warning("Failed to calculate Germany Gmail percentage")
            return 0.0

    def get_top_gmail_countries(self, limit: int = 3) -> List[Dict[str, Any]]:
        """
        Find the top countries using Gmail as email provider.
        
        Args:
            limit: Number of top countries to return
            
        Returns:
            List of country statistics with rank, name, and count
        """
        query = """
        WITH gmail_countries AS (
            SELECT 
                country,
                user_count,
                RANK() OVER (ORDER BY user_count DESC) AS rank
            FROM email_by_country
            WHERE email_provider = 'gmail.com'
        )
        SELECT rank, country, user_count
        FROM gmail_countries
        WHERE rank <= $limit
        ORDER BY rank
        """
        
        result = self.storage.execute_query(query, {"limit": limit})
        
        if result:
            logger.debug(f"Top {limit} countries using Gmail: {result}")
            return result
        else:
            logger.warning(f"Failed to find top {limit} Gmail countries")
            return []

    def get_seniors_with_gmail(self, age_threshold: int = 60) -> int:
        """
        Count people over the specified age using Gmail.
        
        Args:
            age_threshold: Minimum age threshold
            
        Returns:
            Count of seniors using Gmail
        """
        # Determine all age groups that are older than the threshold
        age_groups = [f"'[{age_threshold + x}-{age_threshold + 10 + x}]'" for x in range(0, 50, 10)]
        age_groups_str = ', '.join(age_groups)
        logger.debug(f"Age group pattern for seniors: {age_groups_str}")
        
        query = f"""
        SELECT COUNT(*) AS senior_count
        FROM {self.table_name}
        WHERE 
            email = 'gmail.com'
            AND birthday IN ({age_groups_str})
        """
        
        result = self.storage.execute_query(query)
        
        if result and 'senior_count' in result[0]:
            count = result[0]['senior_count']
            logger.debug(f"People over {age_threshold} using Gmail: {count}")
            return count
        else:
            logger.warning(f"Failed to count seniors over {age_threshold} using Gmail")
            return 0

    def generate_full_report(self) -> Dict[str, Any]:
        """
        Generate a complete report with all metrics.
        
        Returns:
            Dictionary with all report metrics
        """
        report = {
            "germany_gmail_percentage": self.get_germany_gmail_percentage(),
            "top_gmail_countries": self.get_top_gmail_countries(limit=3),
            "seniors_with_gmail": self.get_seniors_with_gmail(age_threshold=60),
            "email_provider_stats": self.storage.get_view_data("email_provider_stats", limit=5),
            "country_stats": self.storage.get_view_data("country_stats", limit=5),
            "age_group_stats": self.storage.get_view_data("age_group_stats")
        }
        
        logger.debug("Generated complete report")
        return report
    
    def save_report_to_json(self, output_path: str) -> bool:
        """
        Generate and save a complete report to a JSON file.
        
        Args:
            output_path: Path to save the JSON report
            
        Returns:
            True if the report was successfully saved, False otherwise
        """
        try:
            # Generate the complete report
            report = self.generate_full_report()
            
            # Create directory if it doesn't exist
            output_dir = os.path.dirname(output_path)
            if output_dir:
                Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Save to JSON file with nice formatting
            with open(output_path, 'w') as f:
                json.dump(report, f, indent=2)
            
            logger.debug(f"Report saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save report: {str(e)}")
            return False
