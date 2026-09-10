import logging
import os

import init_sync
import push_file_to_lz
import utils
from constants import (
    FILE_NAME_LENGTH,
    INIT_SYNC_STATUS_FILE_NAME,
    INIT_SYNC_LAST_ID_FILE_NAME,
    INIT_SYNC_MAX_ID_FILE_NAME,
    LAST_PARQUET_FILE_NUMBER,
)


class _FakeAggResult:
    def __init__(self, value):
        self._value = value
        self._consumed = False

    def next(self):
        if self._consumed or self._value is None:
            raise StopIteration
        self._consumed = True
        return {"_id": self._value}


class _FakeCursor:
    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, *args, **kwargs):
        return self

    def limit(self, n):
        return self

    def __iter__(self):
        return iter(self._docs)


class _FakeCollection:
    def __init__(self, docs, max_id):
        self._docs = docs
        self._max_id = max_id

    def estimated_document_count(self):
        return len(self._docs)

    def aggregate(self, pipeline):
        return _FakeAggResult(self._max_id)

    def find(self, query=None):
        query = query or {}
        docs = self._docs
        id_filter = query.get("_id")
        if id_filter:
            gt = id_filter.get("$gt")
            lte = id_filter.get("$lte")
            docs = [
                d for d in docs
                if (gt is None or d["_id"] > gt) and (lte is None or d["_id"] <= lte)
            ]
        return _FakeCursor(docs)


def _install_fake_mongo(monkeypatch, collection):
    class _FakeDb(dict):
        def __getitem__(self, name):
            return collection

    class _FakeClient:
        def __getitem__(self, name):
            return _FakeDb()

    monkeypatch.setattr(init_sync.pymongo, "MongoClient", lambda *a, **k: _FakeClient())


# ---------------------------------------------------------------------------
# __get_max_id
# ---------------------------------------------------------------------------

def test_get_max_id_returns_max_from_aggregate():
    collection = _FakeCollection(docs=[], max_id=42)
    logger = logging.getLogger("test")

    assert init_sync.__get_max_id(collection, logger) == 42


def test_get_max_id_returns_none_for_empty_collection():
    collection = _FakeCollection(docs=[], max_id=None)
    logger = logging.getLogger("test")

    assert init_sync.__get_max_id(collection, logger) is None


# ---------------------------------------------------------------------------
# init_sync
# ---------------------------------------------------------------------------

def test_init_sync_skips_entirely_when_already_done(monkeypatch):
    monkeypatch.setattr(
        init_sync, "read_from_file",
        lambda table, name, ftype: "Y" if name == INIT_SYNC_STATUS_FILE_NAME else None,
    )

    def fail_if_called(*a, **k):
        raise AssertionError("must not connect to MongoDB once init sync is already done")

    monkeypatch.setattr(init_sync.pymongo, "MongoClient", fail_if_called)

    init_sync.init_sync("mycol")  # should return immediately without error


def test_init_sync_resumes_from_last_id_and_completes(monkeypatch):
    docs = [
        {"_id": 1, "value": "a"},
        {"_id": 2, "value": "b"},
        {"_id": 3, "value": "c"},
    ]
    collection = _FakeCollection(docs=docs, max_id=3)
    _install_fake_mongo(monkeypatch, collection)

    file_state = {
        INIT_SYNC_STATUS_FILE_NAME: "N",
        INIT_SYNC_LAST_ID_FILE_NAME: 2,  # interrupted after _id=2
        INIT_SYNC_MAX_ID_FILE_NAME: None,
        LAST_PARQUET_FILE_NUMBER: None,  # forces the directory-scan fallback
    }
    monkeypatch.setattr(init_sync, "read_from_file", lambda table, name, ftype: file_state.get(name))

    written = []
    monkeypatch.setattr(init_sync, "write_to_file", lambda obj, table, name, ftype: written.append((name, obj)))
    deleted = []
    monkeypatch.setattr(init_sync, "delete_file", lambda table, name: deleted.append(name))
    pushed = []
    monkeypatch.setattr(init_sync, "push_file_to_lz", lambda path, table: pushed.append(path))
    monkeypatch.setattr(init_sync.schema_utils, "process_dataframe", lambda table, df, **k: None)
    monkeypatch.setattr(init_sync.time, "sleep", lambda secs: None)

    init_sync.init_sync("mycol")

    # Only the remaining doc (_id=3) should have been processed as a new batch.
    assert pushed  # a parquet file was pushed to the LZ
    last_id_writes = [obj for name, obj in written if name == INIT_SYNC_LAST_ID_FILE_NAME]
    assert last_id_writes == [3]
    status_writes = [obj for name, obj in written if name == INIT_SYNC_STATUS_FILE_NAME]
    assert status_writes[-1] == "Y"
    assert deleted == [INIT_SYNC_LAST_ID_FILE_NAME]


def test_init_sync_uses_lz_parquet_counter_fallback_when_missing(monkeypatch):
    docs = [{"_id": 1, "value": "a"}]
    collection = _FakeCollection(docs=docs, max_id=1)
    _install_fake_mongo(monkeypatch, collection)

    file_state = {
        INIT_SYNC_STATUS_FILE_NAME: None,
        INIT_SYNC_LAST_ID_FILE_NAME: None,
        INIT_SYNC_MAX_ID_FILE_NAME: None,
        LAST_PARQUET_FILE_NUMBER: None,  # persisted counter missing -> LZ fallback
    }
    monkeypatch.setattr(init_sync, "read_from_file", lambda table, name, ftype: file_state.get(name))
    monkeypatch.setattr(init_sync, "write_to_file", lambda obj, table, name, ftype: None)
    monkeypatch.setattr(init_sync, "delete_file", lambda table, name: None)
    monkeypatch.setattr(init_sync, "push_file_to_lz", lambda path, table: None)
    monkeypatch.setattr(init_sync.schema_utils, "process_dataframe", lambda table, df, **k: None)
    monkeypatch.setattr(init_sync.time, "sleep", lambda secs: None)

    fallback_calls = []

    def spy_fallback(table_name):
        fallback_calls.append(table_name)
        return 0

    monkeypatch.setattr(init_sync, "get_last_parquet_file_num_from_lz", spy_fallback)

    init_sync.init_sync("mycol")

    assert fallback_calls == ["mycol"]


def test_init_sync_fresh_lz_starts_numbering_at_1_ignoring_stale_local_files(monkeypatch):
    """Regression for the fixes-branch bug where a fresh Fabric mirror (empty LZ)
    plus stale local parquet files made init sync resume numbering mid-sequence
    (e.g. ...0104.parquet), which Fabric - expecting ...0001.parquet - ignored.
    The LZ-derived fallback must ignore local files entirely."""
    docs = [{"_id": 1, "value": "a"}]
    collection = _FakeCollection(docs=docs, max_id=1)
    _install_fake_mongo(monkeypatch, collection)

    # Stale local parquet files left over from a previous run / a torn-down mirror.
    table_dir = utils.get_table_dir("mycol")
    for num in (101, 102, 103):
        open(os.path.join(table_dir, str(num).zfill(FILE_NAME_LENGTH) + ".parquet"), "wb").close()

    file_state = {
        INIT_SYNC_STATUS_FILE_NAME: None,
        INIT_SYNC_LAST_ID_FILE_NAME: None,
        INIT_SYNC_MAX_ID_FILE_NAME: None,
        LAST_PARQUET_FILE_NUMBER: None,  # fresh LZ: persisted counter is a 404
    }
    monkeypatch.setattr(init_sync, "read_from_file", lambda table, name, ftype: file_state.get(name))
    written = []
    monkeypatch.setattr(init_sync, "write_to_file", lambda obj, table, name, ftype: written.append((name, obj)))
    monkeypatch.setattr(init_sync, "delete_file", lambda table, name: None)
    pushed = []
    monkeypatch.setattr(init_sync, "push_file_to_lz", lambda path, table: pushed.append(path))
    monkeypatch.setattr(init_sync.schema_utils, "process_dataframe", lambda table, df, **k: None)
    monkeypatch.setattr(init_sync.time, "sleep", lambda secs: None)
    # Fresh LZ: listing the collection's LZ folder returns nothing. Exercise the
    # REAL get_last_parquet_file_num_from_lz so the test proves local files are ignored.
    monkeypatch.setattr(push_file_to_lz, "list_files_in_lz", lambda table: [])

    init_sync.init_sync("mycol")

    parquet_pushes = [
        os.path.basename(p) for p in pushed if os.path.basename(p)[:1].isdigit()
    ]
    assert parquet_pushes == ["00000000000000000001.parquet"]
    counter_writes = [obj for name, obj in written if name == LAST_PARQUET_FILE_NUMBER]
    assert counter_writes == [1]
