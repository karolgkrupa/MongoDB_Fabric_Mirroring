"""Shared pytest fixtures and test doubles.

Everything external to this codebase (MongoDB, the Fabric Landing Zone HTTP
API, and the real data_files/ directory) is mocked or redirected here so the
test suite never touches a real network or a real MongoDB deployment.
"""
import requests
import pytest

import utils
import schemas


# Sane defaults for every env var read anywhere in the codebase via os.getenv().
# Individual tests can override any of these with monkeypatch.setenv(...).
REQUIRED_ENV_DEFAULTS = {
    "MONGO_CONN_STR": "mongodb://localhost:27017",
    "MONGO_DB_NAME": "test_db",
    "LZ_URL": "https://fake-lz.example.com/",
    "APP_ID": "fake-app-id",
    "SECRET": "fake-secret",
    "TENANT_ID": "fake-tenant-id",
    "INIT_LOAD_BATCH_SIZE": "1000",
    "DELTA_SYNC_BATCH_SIZE": "5",
    "TIME_THRESHOLD_IN_SEC": "60",
}


@pytest.fixture(autouse=True)
def _env_defaults(monkeypatch):
    for key, value in REQUIRED_ENV_DEFAULTS.items():
        monkeypatch.setenv(key, value)


@pytest.fixture(autouse=True)
def tmp_data_dir(tmp_path, monkeypatch):
    """Redirect every table directory at a throwaway tmp_path instead of the
    real data_files/ directory.

    utils.py builds table dirs via os.path.join(current_dir, DATA_FILES_PATH,
    ...); os.path.join treats an absolute second component as an override
    (discarding everything before it), so pointing utils.DATA_FILES_PATH at an
    absolute tmp_path redirects every table dir there. utils.py already has
    `DATA_FILES_PATH` bound into its own namespace via `from constants import
    DATA_FILES_PATH`, so the patch target is utils.DATA_FILES_PATH, not
    constants.DATA_FILES_PATH.
    """
    monkeypatch.setattr(utils, "DATA_FILES_PATH", str(tmp_path))
    return tmp_path


@pytest.fixture(autouse=True)
def reset_schemas_module():
    """schemas.py keeps all state in module-level dicts; make sure state from
    one test can never leak into another."""
    schemas.__schemas.clear()
    schemas.__locks.clear()
    schemas.__column_renamings.clear()
    yield
    schemas.__schemas.clear()
    schemas.__locks.clear()
    schemas.__column_renamings.clear()


class FakeResponse:
    """Minimal stand-in for requests.Response used to mock Azure/Fabric HTTP
    calls (LZ reads/writes) without any real network access."""

    def __init__(self, status_code=200, content=b"", text=None, json_data=None):
        self.status_code = status_code
        self.content = content
        if text is not None:
            self.text = text
        elif isinstance(content, (bytes, bytearray)):
            self.text = content.decode("utf-8", errors="ignore")
        else:
            self.text = str(content)
        self._json_data = json_data

    def json(self):
        if self._json_data is not None:
            return self._json_data
        import json as _json
        return _json.loads(self.text)

    def raise_for_status(self):
        if 400 <= self.status_code:
            raise requests.HTTPError(
                f"{self.status_code} Error for url", response=self
            )


@pytest.fixture
def make_response():
    """Factory fixture: make_response(status_code=200, content=b"...") -> FakeResponse"""
    return FakeResponse


class FakeChangeStream:
    """Minimal stand-in for a pymongo ChangeStream cursor.

    `events` is a list consumed in order by try_next(): plain dict-like items
    are returned as-is; BaseException instances are raised instead. Once
    exhausted, try_next() returns None forever (mirroring a live-but-idle
    change stream) unless `then_raise` is provided, in which case that
    exception is raised on every call once the queue is empty.
    """

    def __init__(self, events=None, alive=True, then_raise=None):
        self._events = list(events or [])
        self.alive = alive
        self._then_raise = then_raise

    def try_next(self):
        if self._events:
            item = self._events.pop(0)
            if isinstance(item, BaseException):
                raise item
            return item
        if self._then_raise is not None:
            raise self._then_raise
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


@pytest.fixture
def fake_change_stream():
    return FakeChangeStream


class FakeCollection:
    """Minimal stand-in for a pymongo Collection, only implementing .watch()
    (used by listening.py). `stream_factory` is called with the same kwargs
    `.watch()` receives and must return a FakeChangeStream (or raise)."""

    def __init__(self, stream_factory):
        self._stream_factory = stream_factory

    def watch(self, **kwargs):
        return self._stream_factory(**kwargs)


@pytest.fixture
def fake_collection():
    return FakeCollection
