from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta

from pipeline.api_client import FakerAPIClient
from pipeline.anonymizer import DataAnonymizer
from pipeline.storage import DuckDBStorage
from pipeline.reporter import ReportGenerator
from pipeline.config import Config


@task(retries=3, retry_delay_seconds=30, cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def fetch_data(config: Config) -> list:
    """
    Task to fetch data from Faker API.
    
    Args:
        config: Pipeline configuration
    
    Returns:
        List of person data dictionaries
    """
    logger = get_run_logger()
    logger.info(f"Fetching data for {config.total_persons} persons from Faker API")
    
    api_client = FakerAPIClient(
        base_url=config.faker_api_url,
        retry_attempts=config.retry_attempts,
        timeout=config.timeout
    )
    
    persons_data = []
    batch_size = min(1000, config.total_persons)
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
    
    return persons_data


@task
def anonymize_data(persons_data: list) -> list:
    """
    Task to anonymize person data.
    
    Args:
        persons_data: List of person data dictionaries
    
    Returns:
        List of anonymized person dictionaries
    """
    logger = get_run_logger()
    logger.info("Anonymizing user data")
    
    anonymizer = DataAnonymizer()
    anonymized_data = anonymizer.anonymize_persons(persons_data)
    logger.info(f"Anonymized {len(anonymized_data)} person records")
    
    return anonymized_data


@task
def store_data(anonymized_data: list, config: Config) -> int:
    """
    Task to store anonymized data in DuckDB.
    
    Args:
        anonymized_data: List of anonymized person dictionaries
        config: Pipeline configuration
    
    Returns:
        Number of stored records
    """
    logger = get_run_logger()
    logger.info(f"Storing anonymized data to {config.output_path}")
    
    Path(config.output_path).parent.mkdir(parents=True, exist_ok=True)
    storage = DuckDBStorage(database_path=config.output_path)
    storage.create_schema()
    total_stored = storage.store_persons(anonymized_data)
    
    logger.info(f"Stored {total_stored} anonymized records to database")
    storage.create_views()
    
    return total_stored


@task
def generate_report(config: Config) -> str:
    """
    Task to generate reports from stored data.
    
    Args:
        config: Pipeline configuration
    
    Returns:
        Path to the generated report file
    """
    logger = get_run_logger()
    logger.info("Generating reports")
    
    storage = DuckDBStorage(database_path=config.output_path)
    reporter = ReportGenerator(storage)
    
    report_path = Path(config.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    
    success = reporter.save_report_to_json(str(report_path))
    if success:
        logger.info(f"Full report saved to {report_path}")
    else:
        logger.error(f"Failed to save report to {report_path}")
    
    return str(report_path)


@flow(name="User Data Anonymization Pipeline", log_prints=True)
def user_data_anonymization_flow(
    total_persons: int = 30000,
    gender: str = "",
    output_path: str = "./data/anonymization.duckdb",
    report_path: str = "./data/report.json"
) -> dict:
    """
    Main flow for the User Data Anonymization Pipeline.
    
    Args:
        total_persons: Number of persons to fetch
        gender: Filter by gender (male/female/empty for all)
        output_path: Path to output database
        report_path: Path to output report
    
    Returns:
        Dictionary with pipeline execution results
    """
    logger = get_run_logger()
    logger.info("Starting User Anonymization data pipeline with Prefect")
    
    config = Config(
        total_persons=total_persons,
        gender=gender,
        output_path=output_path,
        report_path=report_path
    )
    
    persons_data = fetch_data(config)
    anonymized_data = anonymize_data(persons_data)
    total_stored = store_data(anonymized_data, config)
    report_path = generate_report(config)
    
    logger.info("Pipeline completed successfully")
    
    return {
        "total_persons": len(persons_data),
        "anonymized_records": len(anonymized_data),
        "stored_records": total_stored,
        "report_path": report_path
    }
