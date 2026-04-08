import logging

from src.streaming.consumer import Consumer
from src.streaming.consumer import DEFAULT_BOOTSTRAP_SERVERS
from src.streaming.consumer import DEFAULT_CITY
from src.streaming.consumer import DEFAULT_KAFKA_TOPIC
from src.streaming.consumer import DEFAULT_LOCAL_STAGING_DIR
from src.streaming.consumer import DEFAULT_OUTPUT_ROOT
from src.streaming.hdfs_client import DEFAULT_HDFS_NAMENODE_URL
from src.streaming.hdfs_client import DEFAULT_HDFS_USER


def main(
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    topic: str = DEFAULT_KAFKA_TOPIC,
    city: str = DEFAULT_CITY,
    output_root: str = DEFAULT_OUTPUT_ROOT,
    processing_date: str | None = None,
    hdfs_namenode_url: str = DEFAULT_HDFS_NAMENODE_URL,
    hdfs_user: str = DEFAULT_HDFS_USER,
    local_staging_dir: str = DEFAULT_LOCAL_STAGING_DIR,
) -> int:
    """Run the streaming consumer entrypoint.

    Args:
        bootstrap_servers: A comma-separated Kafka bootstrap server list.
        topic: A Kafka topic name.
        city: A city name used in output paths.
        output_root: An HDFS root output path.
        processing_date: An optional fallback processing date.
        hdfs_namenode_url: A Namenode WebHDFS base URL.
        hdfs_user: An HDFS user name.
        local_staging_dir: A local staging directory for Parquet part files.

    Returns:
        int: A process exit code.
    """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    consumer = Consumer(
        aqicn_api_token="",
        city=city,
        kafka_bootstrap_servers=bootstrap_servers,
        kafka_topic=topic,
        output_root=output_root,
        processing_date=processing_date,
        hdfs_namenode_url=hdfs_namenode_url,
        hdfs_user=hdfs_user,
        local_staging_dir=local_staging_dir,
    )
    try:
        consumer.run()
    except KeyboardInterrupt:
        logging.getLogger("air_quality.streaming").info(f"Streaming consumer stopped")

    return 0


def run_cli() -> int:
    """Run the streaming module when it is executed as a script.

    Returns:
        int: A process exit code.
    """

    return main()


if __name__ == "__main__":
    raise SystemExit(run_cli())
