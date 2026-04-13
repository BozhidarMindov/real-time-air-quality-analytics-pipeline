from src.streaming.hdfs_client import HDFSClient


class FakeResponse:
    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeWebHDFSSession:
    def __init__(self):
        self.calls = []
        self.get_response = FakeResponse(status_code=200)
        self.put_responses = []
        self.post_responses = []

    def get(self, url, **kwargs):
        self.calls.append(("get", url, kwargs))
        return self.get_response

    def put(self, url, **kwargs):
        self.calls.append(("put", url, kwargs))
        return self.put_responses.pop(0)

    def post(self, url, **kwargs):
        self.calls.append(("post", url, kwargs))
        return self.post_responses.pop(0)


def test_hdfs_client_exists_returns_false_for_missing_path():
    session = FakeWebHDFSSession()
    session.get_response = FakeResponse(status_code=404)
    client = HDFSClient(
        namenode_url="http://namenode:9870",
        user="hdfs",
        session=session,
    )

    result = client.exists("/data/air-quality/sofia/raw/2026-04-07.jsonl")

    assert result is False
    assert session.calls == [
        (
            "get",
            "http://namenode:9870/webhdfs/v1/data/air-quality/sofia/raw/2026-04-07.jsonl",
            {
                "params": {"op": "GETFILESTATUS", "user.name": "hdfs"},
                "allow_redirects": False,
                "timeout": 30,
            },
        )
    ]


def test_hdfs_client_create_text_follows_namenode_redirect_to_datanode():
    session = FakeWebHDFSSession()
    session.put_responses = [
        FakeResponse(
            status_code=307,
            headers={"Location": "http://datanode:9864/webhdfs/v1/data/file"},
        ),
        FakeResponse(status_code=201),
    ]
    client = HDFSClient(
        namenode_url="http://namenode:9870",
        user="hdfs",
        session=session,
    )

    client.create_text("/data/file", "hello\n")

    assert session.calls == [
        (
            "put",
            "http://namenode:9870/webhdfs/v1/data/file",
            {
                "params": {"op": "CREATE", "user.name": "hdfs", "overwrite": "false"},
                "allow_redirects": False,
                "timeout": 30,
            },
        ),
        (
            "put",
            "http://datanode:9864/webhdfs/v1/data/file",
            {
                "data": b"hello\n",
                "timeout": 30,
            },
        ),
    ]


def test_hdfs_client_append_text_follows_namenode_redirect_to_datanode():
    session = FakeWebHDFSSession()
    session.post_responses = [
        FakeResponse(
            status_code=307,
            headers={"Location": "http://datanode:9864/webhdfs/v1/data/file"},
        ),
        FakeResponse(status_code=200),
    ]
    client = HDFSClient(
        namenode_url="http://namenode:9870",
        user="hdfs",
        session=session,
    )

    client.append_text("/data/file", "world\n")

    assert session.calls == [
        (
            "post",
            "http://namenode:9870/webhdfs/v1/data/file",
            {
                "params": {"op": "APPEND", "user.name": "hdfs"},
                "allow_redirects": False,
                "timeout": 30,
            },
        ),
        (
            "post",
            "http://datanode:9864/webhdfs/v1/data/file",
            {
                "data": b"world\n",
                "timeout": 30,
            },
        ),
    ]
