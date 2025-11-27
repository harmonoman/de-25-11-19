# de-25-11-19 - The "Unstable" API Ingestion Challenge

This project implements a robust data ingestion system designed to interact with an unstable, rate-limited, error-prone API. The goal is to simulate real-world data engineering challenges such as authentication, retries, logging, pagination, and reliable delivery to S3.

This repository currently contains:

- A production-friendly authentication module
- A structured application logger (console + rotating file logs)
- A working authenticated request flow that interacts with the `/customers` API endpoint
- Foundation setup for the upcoming Unstable API Ingestion Challenge

## Project Structure
```
src/
│── auth_client.py      # Handles token-based auth & refresh
│── logger.py           # AppLogger class (console + file logging)
│── main.py             # Demonstration script using AuthClient + making authenticated calls
│── logs/               # Rotating logs (auto-generated)
│── ...
README.md
```

## Authentication Module
### AuthClient

AuthClient implements:

- POST `/login` to exchange username/password for a JWT
- Automatic token refresh using `expires_in`
- Error handling + logging of failures
- `get_auth_header()` helper for downstream API calls

This component is functioning correctly and is ready for reuse in the ingestion pipeline.

## Logging Module
### AppLogger

AppLogger implements:

- Namespaced per module (e.g., `"ingestion"`)
- Writes DEBUG+ to console
- Writes ERROR+ to a rotating file (`logs/app.log`)
- JSON-like formatting for machine-readable file logs
- Prevents duplicate handlers

This logger is ready for long-running ingestion jobs.

## Completed Work

Phase 1: Authentication Setup of the assignment.

Achievements:

- Correctly hit the `/login` endpoint
- Retrieved and refreshed JWTs
- Passed authentication headers into API calls
- Tested the `/customers` endpoint
- Confirmed pagination works using the `limit=` parameter

At this stage, you can reliably and repeatedly authenticate into the API.

## Next Step: The “Unstable API Ingestion Challenge”

Upcoming tasks (Phase 2):

- Architectural Decisions:
    * Choose output format: CSV, JSON, NDJSON, or Parquet
    * Choose cleaning strategy: ETL (clean during ingestion) vs ELT (raw dump)
- Implement retry logic with exponential backoff + jitter
- Implement pagination loop using metadata fields
- Implement file writing strategy (streaming or batching)
- Upload final file to S3 under the proper partitioned directory
- Generate an execution report

These pieces will go into a new module (likely `ingest.py`).

## How to Run the Current Script

Activate your virtual environment:

`source .venv/bin/activate`

Run the main script:

`python3 src/main.py`

This will:

- Initialize a logger
- Authenticate against the API
- Fetch the first page of `/customers`
- Print the returned JSON

## Notes & Requirements for Future Work

- The ingestion script must tolerate 500/503 errors
- Must gracefully handle 429 rate limiting
- Must avoid storing 100k records in memory
- Must produce one final file for S3
- Must generate a summary report at the end

All retry, pagination, cleaning, and storage logic will be implemented outside the `AuthClient`.