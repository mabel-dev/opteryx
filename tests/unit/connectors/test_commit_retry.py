import asyncio

from opteryx.connectors.gcp_cloudstorage_connector import GcpCloudStorageConnector
from opteryx.models.query_statistics import QueryStatistics


class FakeResponse:
    def __init__(self, data: bytes, status: int = 200):
        self._data = data
        self.status = status

    async def read(self):
        return self._data


class FakeSession:
    def __init__(self, data: bytes):
        self._data = data

    async def get(self, url, headers=None, timeout=None):
        return FakeResponse(self._data, status=200)


class FlakyPool:
    """Simulate MemoryPool.commit returning -1 first, then a valid ref."""

    def __init__(self):
        self._calls = 0

    async def commit(self, data):
        self._calls += 1
        # first call fails
        if self._calls == 1:
            return -1
        return 123


def test_async_read_blob_retry_on_commit_failure():
    # Arrange
    connector = type("C", (), {})()
    connector.access_token = "tok"

    data = b"hello world"
    session = FakeSession(data)
    pool = FlakyPool()
    stats = QueryStatistics("test")

    # Act
    result = asyncio.run(
        GcpCloudStorageConnector.async_read_blob(
            connector,
            blob_name="bucket/path/file.txt",
            pool=pool,
            session=session,
            statistics=stats,
        )
    )

    # Assert
    assert result == 123
    # commit failed once -> our new stat should have been incremented
    assert stats.stalls_io_waiting_on_engine >= 1
    assert stats.bytes_read >= len(data)
