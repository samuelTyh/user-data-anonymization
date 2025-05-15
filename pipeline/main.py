"""
User Anonymization data pipeline main entry point.
"""
import argparse
import logging
import sys
from pathlib import Path

from .api_client import FakerAPIClient
from .anonymizer import DataAnonymizer
from .storage import DuckDBStorage
from .reporter import ReportGenerator
from .config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

def run_pipeline(config: Config):
    """
    Execute the complete data pipeline.
    
    Args:
        config: Pipeline configuration
    """
    logger.info("Starting User Anonymization data pipeline")
    
    # Step 1: Initialize API client
    api_client = FakerAPIClient(
        base_url=config.faker_api_url,
        retry_attempts=config.retry_attempts,
        timeout=config.timeout
    )
    
    # Step 2: Fetch data in batches
    logger.info(f"Fetching data for {config.total_persons} persons from Faker API")
    persons_data = []
    
    batch_size = min(1000, config.total_persons)  # API might have limitations
    remaining = config.total_persons
    
    while remaining > 0:
        current_batch = min(batch_size, remaining)
        logger.info(f"Fetching batch of {current_batch} persons")
        
        batch_data = api_client.get_persons(
            quantity=current_batch,
            gender=config.gender,
            birthday_start=config.birthday_start
        )
        
        persons_data.extend(batch_data)
        remaining -= current_batch
        logger.info(f"Fetched {len(persons_data)}/{config.total_persons} persons")
        logger.debug(f"{persons_data[-1]}")
    
    # Step 3: Anonymize the data
    logger.info("Anonymizing user data")
    anonymizer = DataAnonymizer()
    anonymized_data = anonymizer.anonymize_persons(persons_data)
    logger.info(f"Anonymized {len(anonymized_data)} person records")
    logger.debug(f"Sample anonymized data: {anonymized_data[:5]}")
    
    # Step 4: Store the data
    logger.info(f"Storing anonymized data to {config.output_path}")
    Path(config.output_path).parent.mkdir(parents=True, exist_ok=True)
    
    storage = DuckDBStorage(database_path=config.output_path)
    storage.create_schema()
    storage.store_persons(anonymized_data)
    
    # Step 5: Create database views for reporting
    logger.info("Creating database views for reporting")
    storage.create_views()
    
    # Step 6: Generate reports and save to JSON
    logger.info("Generating reports")
    reporter = ReportGenerator(storage)
    report_path = Path(config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    reporter.save_report_to_json(str(report_path))
    logger.info(f"Full report saved to {report_path}")
    
    logger.info("Pipeline completed successfully")

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="User Anonymization Data Pipeline")
    
    parser.add_argument(
        "--persons", 
        type=int, 
        default=30000,
        help="Number of persons to fetch (default: 30000)"
    )
    
    parser.add_argument(
        "--gender", 
        type=str, 
        choices=["male", "female", ""], 
        default="",
        help="Filter by gender (leave empty for all)"
    )
    
    parser.add_argument(
        "--output", 
        type=str, 
        default="./data/anonymization.duckdb",
        help="Output database path (default: ./data/anonymization.duckdb)"
    )

    parser.add_argument(
        "--report", 
        type=str, 
        default="./data/report.json",
        help="Output report path (default: ./data/report.json)"
    )
    
    return parser.parse_args()

def main():
    """Main entry point."""
    args = parse_args()
    
    config = Config(
        total_persons=args.persons,
        gender=args.gender,
        output_path=args.output,
        report_path=args.report
    )
    
    run_pipeline(config)

if __name__ == "__main__":
    main()
