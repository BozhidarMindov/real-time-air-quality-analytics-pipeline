import time
import requests


class AQICNClient:
    """A client that fetches raw AQICN air quality payloads for a city.

    Attributes:
        api_token: The AQICN API token used in feed requests.
        base_url: The normalized AQICN feed URL prefix.
        request_timeout_seconds: The timeout applied to each HTTP request.
        retry_attempts: The maximum number of fetch attempts for one call.
        retry_backoff_seconds: The delay between retry attempts.
        session: The request helper used for AQICN calls.
        sleep: The sleep function used between retry attempts.
    """
    def __init__(
        self,
        api_token: str,
        base_url: str = "https://api.waqi.info/feed",
        request_timeout_seconds: int = 30,
        retry_attempts: int = 3,
        retry_backoff_seconds: int = 5,
        session=None,
        sleep=time.sleep,
    ) -> None:
        """Initialize the AQICN client.

        Args:
            api_token: An AQICN API token.
            base_url: A base AQICN feed URL.
            request_timeout_seconds: A request timeout in seconds.
            retry_attempts: A number of retry attempts for transient failures.
            retry_backoff_seconds: A delay between retries in seconds.
            session: An optional request helper. When omitted, a default helper is created.
            sleep: A sleep function used between retries.
        """
        self.api_token = api_token
        self.base_url = base_url.rstrip("/")
        self.request_timeout_seconds = request_timeout_seconds
        self.retry_attempts = retry_attempts
        self.retry_backoff_seconds = retry_backoff_seconds
        if session is None:
            session = requests.Session()
        self.session = session
        self.sleep = sleep

    def fetch_city_feed(self, city: str) -> dict:
        """Fetches the AQICN payload for a city.

        Args:
            city: A city identifier used in the AQICN feed URL.

        Returns:
            The raw AQICN response payload.

        Raises:
            ValueError: A token error when the API token is missing.
            RuntimeError: An AQICN status error when the API response is not successful.
            Exception: The last request or response error after the final retry fails.
        """
        if not self.api_token:
            raise ValueError("AQICN_API_TOKEN is required")

        url = f"{self.base_url}/{city}/"
        params = {"token": self.api_token}

        for attempt in range(1, self.retry_attempts + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.request_timeout_seconds)
                response.raise_for_status()
                payload = response.json()
                if payload.get("status") != "ok":
                    raise RuntimeError(f"AQICN API returned status {payload.get('status')}")
                return payload
            except Exception:
                if attempt >= self.retry_attempts:
                    raise
                self.sleep(self.retry_backoff_seconds)

        raise RuntimeError("AQICN fetch failed unexpectedly")
