import importlib
import os


def _getenv_or_default(key: str, default: str | None) -> str | None:
    value = os.getenv(key)
    return value if value else default


def _load_streaming_module():
    return importlib.import_module("src.streaming.streaming_job")


def main() -> int:
    streaming_module = _load_streaming_module()
    return streaming_module.main(
        bootstrap_servers=_getenv_or_default(
            "KAFKA_BOOTSTRAP_SERVERS",
            streaming_module.DEFAULT_BOOTSTRAP_SERVERS,
        ),
        topic=_getenv_or_default(
            "KAFKA_TOPIC",
            streaming_module.DEFAULT_KAFKA_TOPIC,
        ),
        city=_getenv_or_default(
            "CITY",
            streaming_module.DEFAULT_CITY,
        ),
        output_root=_getenv_or_default(
            "OUTPUT_ROOT",
            streaming_module.DEFAULT_OUTPUT_ROOT,
        ),
        processing_date=_getenv_or_default("PROCESSING_DATE", None),
        hdfs_namenode_url=_getenv_or_default(
            "HDFS_NAMENODE_URL",
            streaming_module.DEFAULT_HDFS_NAMENODE_URL,
        ),
        hdfs_user=_getenv_or_default(
            "HDFS_USER",
            streaming_module.DEFAULT_HDFS_USER,
        ),
        local_staging_dir=_getenv_or_default(
            "LOCAL_STAGING_DIR",
            streaming_module.DEFAULT_LOCAL_STAGING_DIR,
        ),
    )


def run_cli() -> int:
    return main()


if __name__ == "__main__":
    raise SystemExit(run_cli())
