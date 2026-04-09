import os


def get_env_or_default(key: str, default: str | None) -> str | None:
    """Return an environment value when present or fall back to a default.

    Args:
        key: The environment variable name to read.
        default: The fallback value used when the variable is missing or empty.

    Returns:
        The configured environment value or the fallback.
    """
    value = os.getenv(key)
    return value if value else default


def get_int_env_or_default(key: str, default: int) -> int:
    """Return an integer environment value when present or fall back to a default.

    Args:
        key: The environment variable name to read.
        default: The fallback value used when the variable is missing or empty.

    Returns:
        The configured integer value or the fallback.
    """
    value = get_env_or_default(key, str(default))
    return int(value)
