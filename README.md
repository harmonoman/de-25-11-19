# de-25-11-19 - The "Unstable" API Ingestion Challenge

A Production-Ready Data Ingestion Pipeline

This project implements a robust, fault-tolerant ingestion system for interacting with an intentionally unstable, rate-limited, error-prone API.
It is designed to simulate real-world data engineering conditions such as:
    - Unreliable endpoints
    - Authentication & token refresh
    - Pagination
    - Retries with backoff / jitter
    - Memory-safe streaming writes
    - Cloud storage delivery (S3)
    - Execution reporting
    - Structured logging for observability

The goal: Ingest large datasets from an unstable API safely, reliably, repeatedly, and without blowing up memory.

## What the System Does


1. A Production-Ready Authentication Layer (`AuthClient`)
    - Exchanges username/password for JWT
    - Handles token refresh automatically
    - Logs failures neatly
    - Injects auth headers into all requests

2. A Structured Application Logger (`AppLogger`)
    - Console logging (DEBUG+)
    - Rotating file logs (logs/app.log, ERROR+)
    - JSON-style format for machine visibility
    - Prevention of duplicate handlers
    - Namespaced module loggers

3. A Fully Functional Unstable API Client (UnstableAPIClient)
    - Retries with exponential backoff + jitter
    - Automatic tracking of:
        * successful_pages
        * failed_pages
        * retry_count
        * records_ingested
    - Pagination via `iterate_all_pages(limit=...)`
    - Graceful handling of:
        * 500 errors
        * 503 errors
        * 429 (rate limits)
    - Resilient logging for every request

4. A Complete Ingestion Pipeline (`ingest.py`)

    The pipeline:
    - Authenticates
    - Iterates through all pages
    - Streams data into a CSV line-by-line
    - Uploads the file to S3 (timestamped key)
    - Generates a professional execution report
    - Returns:
        * Local CSV path
        * S3 URI

5. A Professional Execution Report

    Example:
    ```
        --- Execution Report ---
    Timestamp: 2025-11-29T20:22:52.575552
    Pages Requested: 125
    Successful Pages: 124
    Failed Pages: 1
    Total Retries: 14
    Records Ingested: 99,400
    Execution Time: 4m 12s
    CSV Output: ./data/customers.csv
    Format Chosen: CSV (Reason: Streaming efficiency)
    ```

## Architectural Decisions
### Decision 1: Output Format = CSV

Reason:

    - CSV supports streaming writes line-by-line.
    - Does not require loading 100,000 records into memory at once.
    - JSON requires the full structure to close properly at the end.
    - Parquet requires batch accumulation and significantly more RAM.
    - CSV is simplest, safest, and avoids memory blow-ups for large datasets.

### Decision 2: Cleaning Strategy = ELT (Clean at the end)

Reason:

    - Ingestion should never fail due to missing or malformed fields.
    - Raw data is preserved exactly as returned from the API.
    - Cleaning can be done later using SQL or Pandas.
    - Clean-on-the-fly risks throwing exceptions mid-ingestion.
    - Prioritizes reliability over immediate cleanliness.

## Project Structure
```
src/
│── utils/
│   ├── auth_client.py         # Authentication + token refresh
│   ├── unstable_api_client.py # Retries, jitter, pagination
│   ├── s3_client.py           # S3 uploads
│   └── logger.py              # Structured logging
│
│── ingest.py                  # Full ingestion pipeline
│── main.py                    # Demo script / dev entrypoint
│
logs/                          # Rotating error logs
data/                          # CSV outputs + reports
README.md
```

## Running the Pipeline
1. Activate your environment
```
source .venv/bin/activate

```

2. Execute ingestion
```
python3 src/ingest.py
```

3. Output

- CSV saved under `./data/...`
- Execution log saved under `./logs/...`
- Report saved under `./data/report_*.txt`
- File uploaded to: `s3://<bucket>/<prefix>/<timestamped_filename>.csv`

## Reliability Requirements (All Implemented)

- Tolerates 500, 503, 429 errors
- Logs every retry and backoff
- Never stores more than ~1–2 pages in memory at once
- Uses streaming CSV writes
- Automatically timestamps and versions S3 uploads
- Emits a professional execution report