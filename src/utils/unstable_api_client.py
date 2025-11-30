import time
import random
import requests


class UnstableAPIClient:
    """
    A reusable client to safely fetch paginated API data from unstable endpoints.
    Handles:
        - retry with exponential backoff + jitter
        - rate-limit handling
        - transient 500/503 failures
        - pagination sequencing
    """


    def __init__(
        self,
        base_url,
        auth_client,
        logger,
        max_retries=5,
        timeout=10,
        jitter=True
    ):
        self.base_url = base_url
        self.auth_client = auth_client
        self.logger = logger
        self.max_retries = max_retries
        self.timeout = timeout
        self.jitter = jitter

        # tracking fields
        self.retry_count = 0
        self.successful_pages = 0
        self.failed_pages = 0
        self.records_ingested = 0

    # ---------------------------------------------------------
    # 1. Fetch a single page (with retry logic)
    # ---------------------------------------------------------
    def fetch_page(self, page, limit=1000):
        """
        Fetch one page of results with retry logic.
        Returns:
            dict -> parsed JSON data (metadata + records)
        """
        url = f"{self.base_url}"
        params = {"page": page, "limit": limit}

        return self._retry_request(url, params)

    # ---------------------------------------------------------
    # 2. Retry Logic (500, 503, 429, network issues)
    # ---------------------------------------------------------
    def _retry_request(self, url, params):
        attempts = 0

        while attempts <= self.max_retries:
            try:
                response = requests.get(
                    url,
                    headers=self.auth_client.get_auth_header(),
                    params=params,
                    timeout=self.timeout
                )

                # SUCCESS
                if response.status_code == 200:
                    return response.json()

                # RATE LIMITED (429)
                if response.status_code == 429:
                    self.retry_count += 1
                    wait = 2 ** attempts
                    if self.jitter:
                        wait += random.uniform(0, 1)
                    self.logger.warning(f"Rate limited: retrying in {wait:.2f}s...")
                    time.sleep(wait)
                    attempts += 1
                    continue

                # SERVER FAILURE (500 or 503)
                if response.status_code in (500, 503):
                    self.retry_count += 1
                    wait = 2 ** attempts
                    if self.jitter:
                        wait += random.uniform(0, 1)
                    self.logger.error(f"Server error {response.status_code}: retrying in {wait:.2f}s...")
                    time.sleep(wait)
                    attempts += 1
                    continue

                # NON-RETRYABLE ERROR
                response.raise_for_status()

            except requests.RequestException as e:
                self.retry_count += 1
                wait = 2 ** attempts
                if self.jitter:
                    wait += random.uniform(0, 1)
                self.logger.error(f"Request failed: {e}, retrying in {wait:.2f}s...")
                time.sleep(wait)
                attempts += 1

        # FAILED ALL RETRIES
        self.logger.error(f"Max retries exceeded for page params: {params}")
        return None

    # ---------------------------------------------------------
    # 3. Generator for all pages (lazy iteration)
    # ---------------------------------------------------------
    def iterate_all_pages(self, limit=1000):
        """
        Automatically yields all pages and tracks:
            - successful pages
            - failed pages
            - total records ingested
        """
        page = 1

        # Fetch first page
        first = self.fetch_page(page, limit)
        if not first:
            self.logger.error("Failed to fetch the first page — cannot continue.")
            self.failed_pages += 1
            return

        total_pages = first["metadata"]["total_pages"]

        # Track success & records
        self.successful_pages += 1
        self.records_ingested += len(first["data"])

        # Yield first
        yield (page, first)

        # Remaining pages
        for page in range(2, total_pages + 1):
            result = self.fetch_page(page, limit)

            if result is None:
                self.failed_pages += 1
                yield (page, None)
                continue

            self.successful_pages += 1
            self.records_ingested += len(result["data"])

            yield (page, result)
