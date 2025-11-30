import time
import requests


class AuthClient:
    """
    Handles authentication & token refresh for an API.
    """

    def __init__(self, auth_url, username, password, logger, timeout=10):
        self.auth_url = auth_url
        self.username = username
        self.password = password
        self.timeout = timeout

        self.logger = logger
        self.access_token = None
        self.expires_at = 0  # epoch timestamp

    def _is_token_expired(self):
        """Check if token is missing or expired."""
        return not self.access_token or time.time() >= self.expires_at

    def _request_new_token(self):
        """Request a new access token from the auth endpoint."""
        try:
            self.logger.info("Requesting new access token...")

            response = requests.post(
                self.auth_url,
                json={
                    "username": self.username,
                    "password": self.password,
                },
                timeout=self.timeout
            )
            response.raise_for_status()
            data = response.json()

            self.access_token = data["access_token"]
            self.expires_at = time.time() + data.get("expires_in", 3600) - 30

            self.logger.info("Authentication successful.")

        except requests.RequestException as e:
            self.logger.error(f"Authentication failed: {e}")
            raise

    def get_token(self):
        """Returns a fresh token. Refreshes automatically if expired."""
        if self._is_token_expired():
            self._request_new_token()
        return self.access_token

    def get_auth_header(self):
        """Helper that returns the Authorization header dict."""
        token = self.get_token()
        return {"Authorization": f"Bearer {token}"}
