import json
import logging
import time

from src.ingestion.aqicn_client import AQICNClient

from kafka import KafkaProducer


class Producer:
    """A producer that publishes raw AQICN payloads to Kafka.

    Args:
        aqicn_api_token: An AQICN API token.
        city: A city name passed to the AQICN feed endpoint.
        poll_interval_seconds: A delay between producer polling attempts.
        kafka_bootstrap_servers: A comma-separated list of Kafka bootstrap servers.
        kafka_topic: A Kafka topic for raw ingestion messages.
        aqicn_base_url: A base AQICN feed URL.
        request_timeout_seconds: A request timeout in seconds.
        retry_attempts: A number of AQICN retry attempts.
        retry_backoff_seconds: A delay between AQICN retry attempts.
    """

    def __init__(
        self,
        aqicn_api_token: str,
        city: str = "sofia",
        poll_interval_seconds: int = 60,
        kafka_bootstrap_servers: str = "localhost:9094",
        kafka_topic: str = "air_quality_sofia",
        aqicn_base_url: str = "https://api.waqi.info/feed",
        request_timeout_seconds: int = 30,
        retry_attempts: int = 3,
        retry_backoff_seconds: int = 5,
    ) -> None:
        self.city = city
        self.poll_interval_seconds = poll_interval_seconds
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.kafka_topic = kafka_topic
        self.aqicn_client = AQICNClient(
            api_token=aqicn_api_token,
            base_url=aqicn_base_url,
            request_timeout_seconds=request_timeout_seconds,
            retry_attempts=retry_attempts,
            retry_backoff_seconds=retry_backoff_seconds,
        )
        self.kafka_producer = self._create_kafka_producer()
        self.logger = logging.getLogger("air_quality.ingestion")
        self.sleep = time.sleep

    def publish_once(self) -> dict:
        """Fetches one payload and publishes it to Kafka.

        Returns:
            dict: A raw AQICN response payload that was published.
        """

        payload = self.aqicn_client.fetch_city_feed(self.city)
        message = json.dumps(payload).encode("utf-8")
        self.kafka_producer.send(self.kafka_topic, value=message)
        self.kafka_producer.flush()
        self.logger.info(f"Published air quality payload for {self.city} to {self.kafka_topic}")
        return payload

    def run(self, iterations: int | None = None) -> None:
        """Runs the producer loop.

        Args:
            iterations: An optional number of iterations for bounded execution.
        """

        completed = 0
        while iterations is None or completed < iterations:
            self.publish_once()
            completed += 1
            if iterations is None or completed < iterations:
                self.sleep(self.poll_interval_seconds)

    def _create_kafka_producer(self):
        """Creates the Kafka producer used by the ingestion producer.

        Returns:
            KafkaProducer: A configured Kafka producer instance.
        """
        bootstrap_servers = [
            server.strip()
            for server in self.kafka_bootstrap_servers.split(",")
            if server.strip()
        ]
        return KafkaProducer(bootstrap_servers=bootstrap_servers)
