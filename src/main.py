"""
Main entry point for the unstable API ingestion pipeline.

This module performs the following functions:

1. Loads environment variables from a .env file, including:
- AWS profile and S3 destinations
- API endpoints and authentication credentials
- Output CSV and report filenames

2. Initializes the application logger.

3. Calls the `run_ingestion_pipeline` function, which:
- Authenticates with the unstable API
- Iteratively retrieves all pages of customer data
- Streams records into a CSV file (memory-efficient)
- Uploads the CSV to S3
- Generates an ingestion summary report

4. Logs the final output path of the generated CSV.

This script is intended to be executed directly and serves as the orchestrator
for the end-to-end ingestion workflow, keeping configuration outside of the code
and inside environment variables for better security and portability.
"""

import os
from dotenv import load_dotenv
from src.utils.logger import AppLogger
from src.ingest import run_ingestion_pipeline


def main():

    load_dotenv()

    logger = AppLogger().get_logger()

    bucket = os.getenv("S3_BUCKET")
    s3_key = os.getenv("S3_KEY")

    api_url = os.getenv("API_URL")
    auth_url = os.getenv("AUTH_URL")
    username = os.getenv("API_USERNAME")
    password = os.getenv("API_PASSWORD")

    csv_filename = os.getenv("CSV_FILENAME")
    report_filename = os.getenv("REPORT_FILENAME")

    output_path = run_ingestion_pipeline(
        bucket,
        s3_key,
        api_url,
        auth_url,
        username,
        password,
        csv_filename,
        report_filename
    )

    logger.info(f"Ingestion completed. CSV saved locally at: {output_path}")


if __name__ == "__main__":
    main()
