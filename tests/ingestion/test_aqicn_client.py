import requests

from src.ingestion.aqicn_client import AQICNClient


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FailingJSONResponse:
    def raise_for_status(self):
        return None

    def json(self):
        raise ValueError("invalid json")


class FailingHTTPResponse:
    def raise_for_status(self):
        raise requests.exceptions.HTTPError("bad gateway")

    def json(self):
        return {"status": "ok"}


class FailingStatusResponse:
    def raise_for_status(self):
        return None

    def json(self):
        return {"status": "error", "data": {}}


class FakeSession:
    def __init__(self, outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_fetch_city_feed_returns_json_and_builds_request():
    payload = {"status": "ok", "data": {"city": {"name": "Sofia"}}}
    session = FakeSession([FakeResponse(payload)])

    client = AQICNClient(api_token="token-123", session=session, sleep=lambda _: None)
    result = client.fetch_city_feed("sofia")

    assert result == payload
    assert session.calls == [
        {
            "url": "https://api.waqi.info/feed/sofia/",
            "params": {"token": "token-123"},
            "timeout": 30,
        }
    ]


def test_fetch_city_feed_retries_after_transient_request_failure():
    payload = {"status": "ok", "data": {"aqi": 42}}
    session = FakeSession(
        [requests.exceptions.ConnectionError("temporary failure"), FakeResponse(payload)]
    )
    sleep_calls = []

    client = AQICNClient(
        api_token="token-123",
        session=session,
        retry_attempts=2,
        retry_backoff_seconds=5,
        sleep=sleep_calls.append,
    )

    result = client.fetch_city_feed("sofia")

    assert result == payload
    assert len(session.calls) == 2
    assert sleep_calls == [5]


def test_fetch_city_feed_retries_after_http_failure():
    payload = {"status": "ok", "data": {"aqi": 42}}
    session = FakeSession(
        [
            FailingHTTPResponse(),
            FakeResponse(payload),
        ]
    )
    sleep_calls = []

    client = AQICNClient(
        api_token="token-123",
        session=session,
        retry_attempts=2,
        retry_backoff_seconds=5,
        sleep=sleep_calls.append,
    )

    result = client.fetch_city_feed("sofia")

    assert result == payload
    assert len(session.calls) == 2
    assert sleep_calls == [5]


def test_fetch_city_feed_retries_after_json_failure():
    payload = {"status": "ok", "data": {"aqi": 42}}
    session = FakeSession([FailingJSONResponse(), FakeResponse(payload)])
    sleep_calls = []

    client = AQICNClient(
        api_token="token-123",
        session=session,
        retry_attempts=2,
        retry_backoff_seconds=5,
        sleep=sleep_calls.append,
    )

    result = client.fetch_city_feed("sofia")

    assert result == payload
    assert len(session.calls) == 2
    assert sleep_calls == [5]


def test_fetch_city_feed_raises_on_api_status_error_without_retry():
    session = FakeSession(
        [FailingStatusResponse(), FailingStatusResponse(), FailingStatusResponse()]
    )
    sleep_calls = []

    client = AQICNClient(
        api_token="token-123",
        session=session,
        retry_attempts=3,
        retry_backoff_seconds=5,
        sleep=sleep_calls.append,
    )

    try:
        client.fetch_city_feed("sofia")
        raise AssertionError("Expected fetch_city_feed to raise RuntimeError")
    except RuntimeError as error:
        assert str(error) == "AQICN API returned status error"

    assert len(session.calls) == 1
    assert sleep_calls == []


def test_fetch_city_feed_does_not_retry_unexpected_runtime_error():
    session = FakeSession(
        [RuntimeError("broken session"), RuntimeError("broken session"), RuntimeError("broken session")]
    )
    sleep_calls = []

    client = AQICNClient(
        api_token="token-123",
        session=session,
        retry_attempts=3,
        retry_backoff_seconds=5,
        sleep=sleep_calls.append,
    )

    try:
        client.fetch_city_feed("sofia")
        raise AssertionError("Expected fetch_city_feed to raise RuntimeError")
    except RuntimeError as error:
        assert str(error) == "broken session"

    assert len(session.calls) == 1
    assert sleep_calls == []
