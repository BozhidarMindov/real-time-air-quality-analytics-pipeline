import json
import logging
import time

from src.ingestion.aqicn_client import AQICNClient

from kafka import KafkaProducer

DEFAULT_POLL_INTERVAL_SECONDS = 300


class Producer:
    """A producer that publishes raw AQICN payloads to Kafka.

    Attributes:
        city: The city requested from the AQICN feed.
        poll_interval_seconds: The delay between producer polling attempts.
        kafka_topic: The Kafka topic that receives raw payloads.
        aqicn_client: The AQICN client used to fetch source payloads.
        kafka_producer: The Kafka producer used to publish payloads.
        logger: The application logger for ingestion events.
        sleep: The sleep function used between polling iterations.
    """

    def __init__(
        self,
        aqicn_client: AQICNClient,
        kafka_producer: KafkaProducer,
        city: str,
        kafka_topic: str,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
        logger: logging.Logger | None = None,
        sleep=time.sleep,
    ) -> None:
        """Initialize the ingestion producer.

        Args:
            aqicn_client: An AQICN client used to fetch source payloads.
            kafka_producer: A Kafka producer used to publish raw payloads.
            city: A city name passed to the AQICN feed endpoint.
            kafka_topic: A Kafka topic for raw ingestion messages.
            poll_interval_seconds: A delay between producer polling attempts.
            logger: An optional application logger.
            sleep: A sleep function used between polling iterations.
        """
        self.aqicn_client = aqicn_client
        self.kafka_producer = kafka_producer
        self.city = city
        self.kafka_topic = kafka_topic
        self.poll_interval_seconds = poll_interval_seconds
        self.logger = logger or logging.getLogger("air_quality.ingestion")
        self.sleep = sleep

    def publish_once(self) -> dict:
        """Fetch one payload and publish it to Kafka.

        Returns:
            The raw AQICN response payload that was published.
        """
        payload = self.aqicn_client.fetch_city_feed(self.city)
        message = json.dumps(payload).encode("utf-8")
        self.kafka_producer.send(self.kafka_topic, value=message)
        self.kafka_producer.flush()
        self.logger.info(
            f"Published air quality payload for {self.city} to {self.kafka_topic}"
        )
        return payload

    def run(self, iterations: int | None = None) -> None:
        """Run the producer loop.

        Args:
            iterations: An optional number of iterations for bounded execution.

        Returns:
            None.
        """
        completed = 0
        while iterations is None or completed < iterations:
            self.publish_once()
            completed += 1
            if iterations is None or completed < iterations:
                self.sleep(self.poll_interval_seconds)
