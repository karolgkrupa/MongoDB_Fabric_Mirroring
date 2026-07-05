import logging
import os
import time

import pandas as pd
import pymongo
import pytest

import listening
import push_file_to_lz
import schemas
import utils
from constants import (
    CHANGE_STREAM_OPERATION_MAP,
    CHANGE_STREAM_OPERATION_MAP_WHEN_INIT,
    DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME,
    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
    FILE_NAME_LENGTH,
    LAST_PARQUET_FILE_NUMBER,
    ROW_MARKER_COLUMN_NAME,
)


# ===========================================================================
# process_accumulative_df
# ===========================================================================

def test_process_accumulative_df_init_phase_flushes_to_temp_parquet():
    df = pd.DataFrame({"_id": [1, 2, 3, 4, 5]})
    logger = logging.getLogger("test")

    result_df, last_sync_time, recovering = listening.process_accumulative_df(
        df, "mycol", "N", None, 60, None, logger,
    )

    assert result_df is None
    table_dir = utils.get_table_dir("mycol")
    temp_files = [f for f in os.listdir(table_dir) if f.startswith("Temp_") and f.endswith(".parquet")]
    assert len(temp_files) == 1


def test_process_accumulative_df_init_phase_does_not_flush_below_batch_size():
    df = pd.DataFrame({"_id": [1]})
    logger = logging.getLogger("test")

    result_df, last_sync_time, recovering = listening.process_accumulative_df(
        df, "mycol", "N", None, 60, None, logger,
    )

    assert result_df is df
    table_dir = utils.get_table_dir("mycol")
    assert not any(f.endswith(".parquet") for f in os.listdir(table_dir))


def test_process_accumulative_df_init_phase_failure_preserves_batch(monkeypatch):
    df = pd.DataFrame({"_id": [1, 2, 3, 4, 5]})
    logger = logging.getLogger("test")

    def boom(self, *a, **k):
        raise OSError("disk full")

    monkeypatch.setattr(pd.DataFrame, "to_parquet", boom)

    result_df, last_sync_time, recovering = listening.process_accumulative_df(
        df, "mycol", "N", None, 60, None, logger,
    )

    # The batch must be retained (not silently dropped) so the next check retries it.
    assert result_df is df


def test_process_accumulative_df_delta_phase_flushes_on_batch_size(monkeypatch):
    pushed = []
    written = []
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: pushed.append(path))
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype, backup_file_name=None: written.append((name, obj, backup_file_name)),
    )
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: 4)

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0] * 5, "_id": [1, 2, 3, 4, 5]})
    logger = logging.getLogger("test")

    result_df, last_sync_time, recovering = listening.process_accumulative_df(
        df, "mycol", "Y", time.time(), 9999, {"tok": 1}, logger,
    )

    assert result_df is None
    assert len(pushed) == 1

    resume_writes = [w for w in written if w[0] == DELTA_SYNC_RESUME_TOKEN_FILE_NAME]
    assert resume_writes == [(DELTA_SYNC_RESUME_TOKEN_FILE_NAME, {"tok": 1}, DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME)]

    counter_writes = [w for w in written if w[0] == LAST_PARQUET_FILE_NUMBER]
    assert counter_writes == [(LAST_PARQUET_FILE_NUMBER, 5, None)]


def test_process_accumulative_df_delta_phase_flushes_on_time_threshold(monkeypatch):
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: None)
    monkeypatch.setattr(listening, "write_to_file", lambda *a, **k: None)
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: 0)

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0], "_id": [1]})
    logger = logging.getLogger("test")

    result_df, last_sync_time, recovering = listening.process_accumulative_df(
        df, "mycol", "Y", time.time() - 100, 1, {"tok": 1}, logger,
    )

    assert result_df is None


def test_process_accumulative_df_falls_back_to_lz_listing_when_counter_missing(monkeypatch):
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: None)
    written = []
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype, backup_file_name=None: written.append((name, obj)),
    )
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: None)
    # LZ already holds up to ...0007.parquet; the fallback derives 7 from the LZ.
    existing_name = str(7).zfill(FILE_NAME_LENGTH) + ".parquet"
    monkeypatch.setattr(push_file_to_lz, "list_files_in_lz", lambda table: [existing_name])

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0], "_id": [1]})
    logger = logging.getLogger("test")

    listening.process_accumulative_df(
        df, "mycol", "Y", time.time(), 9999, {"tok": 1}, logger, force_flush=True,
    )

    counter_writes = [obj for name, obj in written if name == LAST_PARQUET_FILE_NUMBER]
    assert counter_writes == [8]


def test_process_accumulative_df_fresh_lz_starts_numbering_at_1_ignoring_stale_local_files(monkeypatch):
    """Regression: a fresh (empty) LZ plus stale local numbered parquet files must
    start delta numbering at ...0001.parquet, not resume from the local max."""
    pushed = []
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: pushed.append(path))
    written = []
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype, backup_file_name=None: written.append((name, obj)),
    )
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: None)

    # Stale local files from a previous run; must be ignored.
    table_dir = utils.get_table_dir("mycol")
    for num in (101, 102, 103):
        open(os.path.join(table_dir, str(num).zfill(FILE_NAME_LENGTH) + ".parquet"), "wb").close()
    # Fresh LZ: listing returns nothing.
    monkeypatch.setattr(push_file_to_lz, "list_files_in_lz", lambda table: [])

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0], "_id": [1]})
    logger = logging.getLogger("test")

    listening.process_accumulative_df(
        df, "mycol", "Y", time.time(), 9999, {"tok": 1}, logger, force_flush=True,
    )

    assert [os.path.basename(p) for p in pushed] == ["00000000000000000001.parquet"]
    counter_writes = [obj for name, obj in written if name == LAST_PARQUET_FILE_NUMBER]
    assert counter_writes == [1]


def test_process_accumulative_df_clears_recovering_flag_after_successful_flush(monkeypatch):
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: None)
    monkeypatch.setattr(listening, "write_to_file", lambda *a, **k: None)
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: 0)

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [4], "_id": [1]})
    logger = logging.getLogger("test")

    result_df, last_sync_time, recovering = listening.process_accumulative_df(
        df, "mycol", "Y", time.time(), 9999, {"tok": 1}, logger,
        recovering_from_backup_token=True, force_flush=True,
    )

    assert recovering is False


def test_process_accumulative_df_delta_phase_failure_preserves_batch(monkeypatch):
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: 0)

    def boom(path, table):
        raise RuntimeError("simulated push-to-LZ HTTP failure")

    monkeypatch.setattr(listening, "push_file_to_lz", boom)
    written = []
    monkeypatch.setattr(listening, "write_to_file", lambda *a, **k: written.append(a))

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0], "_id": [1]})
    logger = logging.getLogger("test")

    result_df, last_sync_time, recovering = listening.process_accumulative_df(
        df, "mycol", "Y", time.time(), 9999, {"tok": 1}, logger, force_flush=True,
    )

    # The batch must be retained for retry; checkpoints must not be written
    # since the push never actually succeeded.
    assert result_df is df
    assert written == []


# ===========================================================================
# __bump_schema_version
# ===========================================================================

def test_bump_schema_version_flushes_bumps_and_reseeds(monkeypatch):
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: 0)
    pushed = []
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: pushed.append(path))
    written = []
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype, backup_file_name=None: written.append((name, obj, backup_file_name)),
    )
    monkeypatch.setattr(listening, "push_table_metadata_files", lambda table: None)
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)

    schemas.init_table_schema_to_mem("mycol", {})

    accumulative_df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0], "_id": [1]})
    offending_doc = {"_id": 2, "new_field": "value"}
    logger = logging.getLogger("test")

    new_df, new_last_sync_time, recovering = listening.__bump_schema_version(
        "mycol", accumulative_df, {"tok": 1}, "Y", time.time(), 9999, offending_doc, logger,
    )

    assert utils.get_schema_version("mycol") == 2
    assert pushed  # old batch flushed before the version bump
    assert new_df[ROW_MARKER_COLUMN_NAME].iloc[0] == CHANGE_STREAM_OPERATION_MAP["insert"]

    schema = schemas.get_table_schema("mycol")
    assert schema is not None
    assert "new_field" in schema

    # Written once by the pre-bump flush (process_accumulative_df) and again
    # explicitly by __bump_schema_version itself to carry it into the new
    # versioned directory - both must use the backup slot.
    resume_writes = [w for w in written if w[0] == DELTA_SYNC_RESUME_TOKEN_FILE_NAME]
    assert resume_writes
    assert all(w == (DELTA_SYNC_RESUME_TOKEN_FILE_NAME, {"tok": 1}, DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME) for w in resume_writes)


def test_bump_schema_version_clears_recovering_flag(monkeypatch):
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: 0)
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: None)
    monkeypatch.setattr(listening, "write_to_file", lambda *a, **k: None)
    monkeypatch.setattr(listening, "push_table_metadata_files", lambda table: None)
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)

    schemas.init_table_schema_to_mem("mycol", {})

    new_df, new_last_sync_time, recovering = listening.__bump_schema_version(
        "mycol", None, {"tok": 1}, "Y", time.time(), 9999, {"_id": 1}, logging.getLogger("test"),
        recovering_from_backup_token=True,
    )

    assert recovering is False


# ===========================================================================
# listening() - main loop: safety-net catch-all + resumable-error escalation
# + row-marker recovery. These drive the real infinite loop and rely on a
# BaseException sentinel (not caught by any `except Exception`/pymongo clause)
# to deterministically stop it after a bounded number of iterations.
# ===========================================================================

class _StopLoop(BaseException):
    """Sentinel used to break out of listening()'s infinite loop in tests.
    Must NOT subclass Exception, or the new safety-net `except Exception`
    clause in listening.py would swallow it instead of letting it propagate.
    """


def _install_fake_collection(monkeypatch, collection):
    class _FakeDb(dict):
        def __getitem__(self, name):
            return collection

    class _FakeClient:
        def __getitem__(self, name):
            return _FakeDb()

    monkeypatch.setattr(listening.pymongo, "MongoClient", lambda *a, **k: _FakeClient())


def _patch_listening_startup(monkeypatch, resume_token=None, used_backup=False, primary_corrupted=False):
    monkeypatch.setattr(listening, "init_sync", lambda collection_name: None)
    monkeypatch.setattr(
        listening, "read_from_file_with_backup",
        lambda *a, **k: (resume_token, used_backup, primary_corrupted),
    )


def test_listening_catchall_safety_net_keeps_thread_alive(monkeypatch, caplog, fake_collection):
    caplog.set_level(logging.CRITICAL)
    calls = {"n": 0}

    def stream_factory(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            # Not a pymongo error - e.g. an HTTP failure raised out of
            # __bump_schema_version's push_file_to_lz call. Must be caught by
            # the new catch-all `except Exception` clause, not kill the thread.
            raise RuntimeError("simulated unexpected failure")
        raise _StopLoop()

    collection = fake_collection(stream_factory)
    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch)
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    # The loop must have reopened the stream and tried again instead of dying.
    assert calls["n"] == 2
    assert any("DATA CONTINUITY RISK" in r.message for r in caplog.records)


def test_listening_resumable_error_escalates_after_threshold(monkeypatch, caplog, fake_collection):
    caplog.set_level(logging.CRITICAL)
    monkeypatch.setenv("RESUMABLE_ERROR_MAX_RETRY_SEC", "0")

    calls = {"n": 0}

    def stream_factory(**kwargs):
        calls["n"] += 1
        if calls["n"] <= 2:
            # A "resumable" error (not code 286 / NonResumableChangeStreamError)
            # that in practice never actually recovers (e.g. HMAC KeyNotFound
            # after a replica-set restart).
            raise pymongo.errors.OperationFailure("No keys found for HMAC", code=211)
        raise _StopLoop()

    collection = fake_collection(stream_factory)
    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch, resume_token={"tok": "stale"})

    written = []
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype, backup_file_name=None: written.append((name, obj)),
    )
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    # 1st failure just starts the streak; 2nd failure (threshold=0) escalates.
    assert calls["n"] == 3

    resume_clears = [obj for name, obj in written if name == DELTA_SYNC_RESUME_TOKEN_FILE_NAME]
    backup_clears = [obj for name, obj in written if name == DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME]
    assert resume_clears == [None]
    assert backup_clears == [None]
    assert any("escalating to a non-resumable reset" in r.message for r in caplog.records)


def test_listening_resumable_error_does_not_escalate_within_window(monkeypatch, fake_collection):
    monkeypatch.setenv("RESUMABLE_ERROR_MAX_RETRY_SEC", "300")

    calls = {"n": 0}

    def stream_factory(**kwargs):
        calls["n"] += 1
        if calls["n"] <= 3:
            raise pymongo.errors.OperationFailure("transient blip", code=None)
        raise _StopLoop()

    collection = fake_collection(stream_factory)
    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch, resume_token={"tok": "still-good"})

    written = []
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype, backup_file_name=None: written.append((name, obj)),
    )
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    # With a large threshold, repeated resumable errors must never escalate.
    assert written == []


def test_listening_recovering_from_backup_token_forces_upsert_for_insert(monkeypatch, fake_change_stream):
    change = {
        "operationType": "insert",
        "fullDocument": {"_id": 123, "value": "hello"},
        "_id": {"_data": "fake-resume-token-blob"},
    }
    stream = fake_change_stream(events=[change], then_raise=_StopLoop())
    collection = type("C", (), {"watch": lambda self, **kwargs: stream})()

    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch, resume_token={"tok": "old"}, used_backup=True)
    monkeypatch.setattr(
        listening, "read_from_file",
        lambda table, name, ftype: "Y" if "init_sync_status" in name else None,
    )
    monkeypatch.setattr(listening.schema_utils, "process_dataframe", lambda table, df: None)
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    captured = {}

    def spy_process_accumulative_df(
        accumulative_df, collection_name, init_sync_stat_flag, last_sync_time,
        time_threshold_in_sec, resume_token, logger, recovering_from_backup_token=False, force_flush=False,
    ):
        captured["df"] = accumulative_df.copy()
        captured["recovering"] = recovering_from_backup_token
        return accumulative_df, last_sync_time, recovering_from_backup_token

    monkeypatch.setattr(listening, "process_accumulative_df", spy_process_accumulative_df)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    assert captured["recovering"] is True
    assert captured["df"][ROW_MARKER_COLUMN_NAME].iloc[0] == CHANGE_STREAM_OPERATION_MAP_WHEN_INIT["insert"]


def test_listening_normal_insert_uses_standard_row_marker_when_not_recovering(monkeypatch, fake_change_stream):
    change = {
        "operationType": "insert",
        "fullDocument": {"_id": 123, "value": "hello"},
        "_id": {"_data": "fake-resume-token-blob"},
    }
    stream = fake_change_stream(events=[change], then_raise=_StopLoop())
    collection = type("C", (), {"watch": lambda self, **kwargs: stream})()

    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch, resume_token=None, used_backup=False)
    monkeypatch.setattr(
        listening, "read_from_file",
        lambda table, name, ftype: "Y" if "init_sync_status" in name else None,
    )
    monkeypatch.setattr(listening.schema_utils, "process_dataframe", lambda table, df: None)
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    captured = {}

    def spy_process_accumulative_df(
        accumulative_df, collection_name, init_sync_stat_flag, last_sync_time,
        time_threshold_in_sec, resume_token, logger, recovering_from_backup_token=False, force_flush=False,
    ):
        captured["df"] = accumulative_df.copy()
        return accumulative_df, last_sync_time, recovering_from_backup_token

    monkeypatch.setattr(listening, "process_accumulative_df", spy_process_accumulative_df)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    assert captured["df"][ROW_MARKER_COLUMN_NAME].iloc[0] == CHANGE_STREAM_OPERATION_MAP["insert"]


# ===========================================================================
# NonResumableChangeStreamError handling
# ===========================================================================

def test_listening_non_resumable_error_clears_resume_token_and_backup(monkeypatch, caplog, fake_collection):
    caplog.set_level(logging.CRITICAL)
    calls = {"n": 0}

    def stream_factory(**kwargs):
        calls["n"] += 1
        if calls["n"] == 1:
            raise pymongo.errors.OperationFailure("change stream history lost", code=286)
        raise _StopLoop()

    collection = fake_collection(stream_factory)
    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch, resume_token={"tok": "stale"})

    written = []
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype, backup_file_name=None: written.append((name, obj)),
    )
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    resume_clears = [obj for name, obj in written if name == DELTA_SYNC_RESUME_TOKEN_FILE_NAME]
    backup_clears = [obj for name, obj in written if name == DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME]
    assert resume_clears == [None]
    assert backup_clears == [None]
    assert any("DATA CONTINUITY RISK" in r.message for r in caplog.records)
