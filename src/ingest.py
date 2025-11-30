"""
ingest.py
---------
Orchestrates ingestion from the unstable API.

This script:
1. Creates the API client + logger
2. Iterates through pages using the client's built-in retry logic
3. Streams all results into a CSV (memory-safe)
4. Uploads the resulting CSV to S3
5. Creates a summary ingestion report
"""

import csv
import os
from datetime import datetime
from src.utils.logger import AppLogger
from src.utils.auth_client import AuthClient
from src.utils.unstable_api_client import UnstableAPIClient
from src.utils.s3_client import S3FileClient

OUTPUT_DIR = "./data"

aws_profile = os.getenv("AWS_PROFILE")


def run_ingestion_pipeline(
    s3_bucket: str,
    s3_key: str,
    api_url: str,
    auth_url: str,
    username: str,
    password: str,
    csv_filename: str = None,
    report_filename: str = None,
) -> tuple[str, str]:
    """
    Run the ingestion pipeline.

    Returns:
        tuple: (local CSV path, S3 URI)
    """
    start_time = datetime.now()

    pages_requested = 0

    # -------------------------
    # SETUP
    # -------------------------
    logger = AppLogger("Ingestion").get_logger()
    logger.info("Starting unstable API ingestion pipeline...")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Filenames
    csv_output = os.path.join(
        OUTPUT_DIR, csv_filename or f"unstable_raw_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    report_output = os.path.join(
        OUTPUT_DIR, report_filename or f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    )

    try:
        # -------------------------
        # AUTH & CLIENT SETUP
        # -------------------------
        auth = AuthClient(
            auth_url=auth_url,
            username=username,
            password=password,
            logger=logger
        )

        client = UnstableAPIClient(
            base_url=api_url,
            auth_client=auth,
            max_retries=5,
            logger=logger,
            timeout=10
        )

        # -------------------------
        # CSV INGESTION
        # -------------------------
        total_rows = 0
        with open(csv_output, "w", newline="", encoding="utf-8") as csvfile:
            writer = None

            for page_num, data in client.iterate_all_pages(limit=1000):
                pages_requested += 1

                if not data:
                    logger.warning(f"Page {page_num} returned no data.")
                    continue

                items = data.get("data") or []
                if not items:
                    logger.info(f"Page {page_num} contained 0 records.")
                    continue

                # --- Discover union of all keys ---
                fieldnames = set()
                for r in items:
                    fieldnames.update(r.keys())
                fieldnames = sorted(fieldnames)

                if writer is None:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()

                # --- Safe write using row.get(key, "N/A") ---
                for row in items:
                    safe_row = {key: row.get(key, "N/A") for key in fieldnames}
                    writer.writerow(safe_row)
                    total_rows += 1

                logger.info(f"Page {page_num} ingested ({len(items)} records, {total_rows} total)")

        # -------------------------
        # UPLOAD TO S3
        # -------------------------
        logger.info("Uploading output to S3...")

        uploader = S3FileClient(logger, aws_profile, region_name="us-west-2")

        # derive folder from provided key
        s3_folder = os.path.dirname(s3_key)  # e.g., "Bond/raw/"

        # new key = folder + timestamped local filename
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        timestamped_key = f"{s3_folder}/{csv_filename}_{timestamp}.csv"

        success = uploader.upload_file(
            local_path=csv_output,
            bucket=s3_bucket,
            key=timestamped_key
        )

        if not success:
            logger.error("S3 upload failed.")
        else:
            logger.info(f"S3 upload complete: s3://{s3_bucket}/{timestamped_key}")

        # -------------------------
        # REPORT GENERATION
        # -------------------------
        execution_time = datetime.now() - start_time

        _generate_report(
            pages_requested=pages_requested,
            successful_pages=client.successful_pages,
            failed_pages=client.failed_pages,
            total_retries=client.retry_count,
            records_ingested=client.records_ingested,
            execution_time=execution_time,
            csv_path=csv_output,
            report_path=report_output,
            logger=logger
        )

        logger.info("Ingestion pipeline finished successfully.")
        return csv_output, f"s3://{s3_bucket}/{timestamped_key}"

    except Exception as e:
        logger.exception(f"Pipeline failed due to an unexpected error {e}")
        raise


def _generate_report(
    pages_requested,
    successful_pages,
    failed_pages,
    total_retries,
    records_ingested,
    execution_time,
    csv_path,
    report_path,
    logger
):
    """Writes a simple ingestion report."""
    logger.info("Generating report...")

    report_text = (
        "--- Execution Report ---\n"
        f"Timestamp: {datetime.now().isoformat()}\n"
        f"Pages Requested: {pages_requested}\n"
        f"Successful Pages: {successful_pages}\n"
        f"Failed Pages: {failed_pages}\n"
        f"Total Retries: {total_retries}\n"
        f"Records Ingested: {records_ingested:,}\n"
        f"Execution Time: {_format_execution_time(execution_time)}\n"
        f"CSV Output: {csv_path}\n"
        "Format Chosen: CSV (Reason: Streaming efficiency)\n"
    )

    with open(report_path, "w") as f:
        f.write(report_text)

    logger.info(f"Report written to {report_path}")


def _format_execution_time(delta):
    total_seconds = int(delta.total_seconds())
    minutes, seconds = divmod(total_seconds, 60)
    return f"{minutes}m {seconds}s"
