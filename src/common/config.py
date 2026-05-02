import os


DEFAULT_KAFKA_BOOTSTRAP_SERVERS = "localhost:9094"


def get_required_env(name: str) -> str:
    """Return a required environment variable value.

    Args:
        name: The environment variable name to read.

    Returns:
        The configured environment variable value.

    Raises:
        ValueError: The environment variable is missing or blank.
    """
    value = os.getenv(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} is required")
    return value


def get_default_kafka_topic(city: str) -> str:
    """Return the default Kafka topic name for a city.

    Args:
        city: The city name used by the AQICN feed and pipeline storage layout.

    Returns:
        The default city-specific Kafka topic name.
    """
    return f"air_quality_{city}"
