import json
import logging
import os
from pathlib import Path
from typing import Dict, List, Any

from storage import DuckDBStorage

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
    
    def get_germany_gmail_percentage(self) -> float:
        """
        Calculate the percentage of users in Germany using Gmail.
        
        Returns:
            Percentage as a float
        """
        query = """
        WITH stats AS (
            SELECT
                COUNT(*) AS total_users,
                COUNT(CASE WHEN country = 'Germany' AND email LIKE '%gmail.com' THEN 1 END) AS germany_gmail_users
            FROM persons
        )
        SELECT 
            (germany_gmail_users * 100 / total_users) AS percentage
        FROM stats
        """
        
        result = self.storage.execute_query(query)
        
        if result and 'percentage' in result[0]:
            percentage = result[0]['percentage']
            logger.info(f"Percentage of users in Germany using Gmail: {percentage:.2f}%")
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
        WITH country_counts AS (
            SELECT
                country,
                COUNT(*) AS user_count
            FROM persons
            WHERE email LIKE '%gmail.com'
            GROUP BY country
            ORDER BY user_count DESC
        ),
        ranked_countries AS (
            SELECT
                country,
                user_count,
                RANK() OVER (ORDER BY user_count DESC) AS rank
            FROM country_counts
        )
        SELECT
            rank,
            country,
            user_count
        FROM ranked_countries
        WHERE rank <= $limit
        ORDER BY rank
        """
        
        result = self.storage.execute_query(query, {"limit": limit})
        
        if result:
            logger.info(f"Top {limit} countries using Gmail: {result}")
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
        # Determine all age groups that are older than the threshold, e.g. 60 -> ([60-70], [70-80], [80-90], [90-100])
        age_group_pattern = [f"'[{age_threshold + x}-{age_threshold + 10 + x}]'" for x in range(0, 50, 10)]
        age_group_pattern = ', '.join(age_group_pattern)
        logger.debug(f"Age group pattern for seniors: {age_group_pattern}")
        
        query = """
        SELECT
            COUNT(*) AS senior_count
        FROM persons
        WHERE 
            email LIKE '%gmail.com'
            AND birthday IN $pattern
        """
        
        result = self.storage.execute_query(query, {"pattern": age_group_pattern})
        
        if result and 'senior_count' in result[0]:
            count = result[0]['senior_count']
            logger.info(f"People over {age_threshold} using Gmail: {count}")
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
            "seniors_with_gmail": self.get_seniors_with_gmail(age_threshold=60)
        }
        
        logger.info("Generated complete report")
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
            
            logger.info(f"Report saved to {output_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save report: {str(e)}")
            return False
