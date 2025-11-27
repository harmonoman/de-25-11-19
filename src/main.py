import requests
from auth_client import AuthClient
from logger import AppLogger


def main():
    # --------------------------------------------------
    # 1. Initialize Logger
    # --------------------------------------------------
    logger = AppLogger(name="ingestion").get_logger()

    # --------------------------------------------------
    # 2. Create AuthClient
    # --------------------------------------------------
    auth = AuthClient(
        auth_url="https://xvserzimz6ofnmxbghdkqpgpma0horhq.lambda-url.us-east-2.on.aws/login",
        username="admin",
        password="password123",
        logger=logger
    )

    # --------------------------------------------------
    # 3. Use the AuthClient to make an authenticated request
    # --------------------------------------------------
    headers = auth.get_auth_header()

    logger.info("Making authenticated API request...")

    response = requests.get(
        "https://xvserzimz6ofnmxbghdkqpgpma0horhq.lambda-url.us-east-2.on.aws/customers",
        headers=headers,
        timeout=10,
        params={"page": 1, "limit": 1000}
    )

    if response.status_code == 200:
        logger.info("Data fetched successfully!")
        print(response.json())
    else:
        logger.error(f"Failed to fetch data: {response.text}")


if __name__ == '__main__':
    main()
