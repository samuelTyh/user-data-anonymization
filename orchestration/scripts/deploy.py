import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from prefect import serve
from orchestration.prefect_flow import user_data_anonymization_flow


if __name__ == "__main__":
    user_data_anonymization_flow.serve(
        name="anonymization-pipeline",
        cron="00/30 * ? * *",  # Run every 30 mins
        parameters={
            "total_persons": 30000,
            "gender": "",
            "output_path": "./data/anonymization.duckdb",
            "report_path": "./data/report.json"
        },
        description="Data anonymization pipeline that fetches from Faker API, anonymizes and generates reports",
        tags=["data-processing", "anonymization"],
    )
