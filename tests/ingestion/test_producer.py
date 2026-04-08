import json

import src.ingestion.producer as producer_module
from src.ingestion.producer import Producer


class FakeAQICNClient:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.requested_cities = []

    def fetch_city_feed(self, city):
        self.requested_cities.append(city)
        return self.payloads.pop(0)


class FakeKafkaProducer:
    def __init__(self):
        self.sent_messages = []
        self.flush_count = 0

    def send(self, topic, value):
        self.sent_messages.append({"topic": topic, "value": value})

    def flush(self):
        self.flush_count += 1


class FakeLogger:
    def __init__(self):
        self.messages = []
        self.calls = []

    def info(self, message, *args):
        self.calls.append((message, args))
        self.messages.append(message % args)


def test_publish_once_sends_raw_json_to_kafka_topic(mocker):
    payload = {"status": "ok", "data": {"aqi": 75}}
    fake_logger = FakeLogger()
    fake_client = FakeAQICNClient([payload])
    fake_kafka_producer = FakeKafkaProducer()

    mocker.patch.object(producer_module, "AQICNClient", return_value=fake_client)
    mocker.patch.object(producer_module.Producer, "_create_kafka_producer", return_value=fake_kafka_producer)

    producer = Producer(aqicn_api_token="token", city="sofia", kafka_topic="air_quality_sofia")
    producer.logger = fake_logger

    producer.publish_once()

    assert producer.kafka_producer.sent_messages[0]["topic"] == "air_quality_sofia"
    assert json.loads(producer.kafka_producer.sent_messages[0]["value"].decode("utf-8")) == payload
    assert producer.kafka_producer.flush_count == 1
    assert producer.logger.messages == ["Published air quality payload for sofia to air_quality_sofia"]
    assert producer.logger.calls == [("Published air quality payload for sofia to air_quality_sofia", ())]


def test_run_sleeps_between_iterations(mocker):
    sleep_calls = []
    fake_client = FakeAQICNClient([{"status": "ok"}, {"status": "ok"}])

    mocker.patch.object(producer_module, "AQICNClient", return_value=fake_client)
    mocker.patch.object(producer_module.Producer, "_create_kafka_producer", return_value=FakeKafkaProducer())

    producer = Producer(aqicn_api_token="token", city="sofia", poll_interval_seconds=120)
    producer.logger = FakeLogger()
    producer.sleep = sleep_calls.append

    producer.run(iterations=2)

    assert sleep_calls == [120]


def test_producer_builds_kafka_producer_from_bootstrap_servers(mocker):
    captured = {}

    class ProducerSpy:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    class FakeAQICNClientClass:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def fetch_city_feed(self, city):
            return {"status": "ok", "city": city}

    mocker.patch.object(producer_module, "KafkaProducer", ProducerSpy)
    mocker.patch.object(producer_module, "AQICNClient", FakeAQICNClientClass)

    producer = Producer(
        aqicn_api_token="token",
        kafka_bootstrap_servers="broker-1:9092,broker-2:9092",
    )

    assert isinstance(producer.kafka_producer, ProducerSpy)
    assert captured["bootstrap_servers"] == ["broker-1:9092", "broker-2:9092"]
