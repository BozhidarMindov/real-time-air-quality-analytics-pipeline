from pathlib import Path
from urllib.parse import quote

import requests


DEFAULT_HDFS_NAMENODE_URL = "http://namenode:9870"
DEFAULT_HDFS_USER = "hdfs"


def normalize_hdfs_path(path: Path | str) -> str:
    """Normalize a local or HDFS path into an absolute HDFS path string.

    Args:
        path: A local-style or HDFS-style path value.

    Returns:
        The absolute HDFS path without a trailing slash.
    """
    value = path.as_posix() if isinstance(path, Path) else str(path)
    value = value.strip()
    if value.startswith("hdfs://"):
        suffix = value.split("://", 1)[1]
        slash_index = suffix.find("/")
        value = suffix[slash_index:] if slash_index >= 0 else "/"
    value = value.replace("\\", "/")
    value = value.rstrip("/")
    if not value.startswith("/"):
        value = f"/{value}"
    return value or "/"


class HDFSClient:
    """A small WebHDFS client used by the streaming consumer.

    Attributes:
        namenode_url: The normalized Namenode WebHDFS base URL.
        user: The HDFS user name sent with requests.
        session: The request helper used for WebHDFS calls.
        timeout_seconds: The timeout applied to each HTTP request.
    """

    def __init__(
        self,
        namenode_url: str = DEFAULT_HDFS_NAMENODE_URL,
        user: str = DEFAULT_HDFS_USER,
        session: requests.Session | None = None,
        timeout_seconds: int = 30,
    ) -> None:
        """Initialize the WebHDFS client.

        Args:
            namenode_url: A Namenode WebHDFS base URL.
            user: An HDFS user name.
            session: An optional request helper.
            timeout_seconds: An HTTP timeout in seconds.
        """
        self.namenode_url = namenode_url.rstrip("/")
        self.user = user
        self.session = session or requests.Session()
        self.timeout_seconds = timeout_seconds

    def exists(self, path: Path | str) -> bool:
        """Return whether a file or directory exists in HDFS.

        Args:
            path: An HDFS path.

        Returns:
            Whether the path exists.
        """
        response = self.session.get(
            self._build_url(path),
            params={"op": "GETFILESTATUS", "user.name": self.user},
            allow_redirects=False,
            timeout=self.timeout_seconds,
        )
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True

    def ensure_directory(self, path: Path | str) -> None:
        """Create a directory in HDFS when it is missing.

        Args:
            path: An HDFS directory path.

        Returns:
            None.
        """
        response = self.session.put(
            self._build_url(path),
            params={"op": "MKDIRS", "user.name": self.user},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

    def create_text(self, path: Path | str, content: str) -> None:
        """Create a new UTF-8 text file in HDFS.

        Args:
            path: An HDFS file path.
            content: A UTF-8 text payload.

        Returns:
            None.
        """
        self._write_bytes(path, content.encode("utf-8"), op="CREATE")

    def append_text(self, path: Path | str, content: str) -> None:
        """Append UTF-8 text to an existing HDFS file.

        Args:
            path: An HDFS file path.
            content: A UTF-8 text payload.

        Returns:
            None.
        """
        self._write_bytes(path, content.encode("utf-8"), op="APPEND")

    def upload_file(self, local_path: Path | str, remote_path: Path | str) -> None:
        """Upload a local file into HDFS.

        Args:
            local_path: A local file path.
            remote_path: An HDFS file path.

        Returns:
            None.
        """
        self._write_bytes(remote_path, Path(local_path).read_bytes(), op="CREATE")

    def _build_url(self, path: Path | str) -> str:
        """Build the Namenode WebHDFS URL for a path.

        Args:
            path: An HDFS path.

        Returns:
            The request URL for the provided path.
        """
        normalized_path = normalize_hdfs_path(path)
        return f"{self.namenode_url}/webhdfs/v1{quote(normalized_path, safe='/')}"

    def _write_bytes(self, path: Path | str, data: bytes, op: str) -> None:
        """Send a CREATE or APPEND request through WebHDFS.

        Args:
            path: An HDFS file path.
            data: A binary payload to write.
            op: A WebHDFS write operation name.

        Returns:
            None.

        Raises:
            RuntimeError: A redirect error when WebHDFS does not return a datanode location.
        """
        request_method = self.session.put if op == "CREATE" else self.session.post
        params = {"op": op, "user.name": self.user}
        if op == "CREATE":
            params["overwrite"] = "false"

        response = request_method(
            self._build_url(path),
            params=params,
            allow_redirects=False,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        redirect_url = response.headers.get("Location")
        if not redirect_url:
            raise RuntimeError(f"WebHDFS {op} did not return a datanode redirect")

        follow_up = request_method(
            redirect_url,
            data=data,
            timeout=self.timeout_seconds,
        )
        follow_up.raise_for_status()
