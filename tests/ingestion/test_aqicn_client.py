from src.ingestion.aqicn_client import AQICNClient


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


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


def test_fetch_city_feed_retries_after_transient_failure():
    payload = {"status": "ok", "data": {"aqi": 42}}
    session = FakeSession([RuntimeError("temporary failure"), FakeResponse(payload)])
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
