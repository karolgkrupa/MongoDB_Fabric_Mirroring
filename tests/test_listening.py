import logging
import os
import time

import pandas as pd
import pymongo
import pytest

import listening
import schemas
import utils
from constants import (
    CHANGE_STREAM_OPERATION_MAP,
    CHANGE_STREAM_OPERATION_MAP_WHEN_INIT,
    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
    LAST_PARQUET_FILE_NUMBER,
    ROW_MARKER_COLUMN_NAME,
)


# ===========================================================================
# process_accumulative_df
# ===========================================================================

def test_process_accumulative_df_init_phase_flushes_to_temp_parquet():
    df = pd.DataFrame({"_id": [1, 2, 3, 4, 5]})
    logger = logging.getLogger("test")

    result_df, last_sync_time = listening.process_accumulative_df(
        df, "mycol", "N", None, 60, None, logger,
    )

    assert result_df is None
    table_dir = utils.get_table_dir("mycol")
    temp_files = [f for f in os.listdir(table_dir) if f.startswith("Temp_") and f.endswith(".parquet")]
    assert len(temp_files) == 1


def test_process_accumulative_df_init_phase_does_not_flush_below_batch_size():
    df = pd.DataFrame({"_id": [1]})
    logger = logging.getLogger("test")

    result_df, last_sync_time = listening.process_accumulative_df(
        df, "mycol", "N", None, 60, None, logger,
    )

    assert result_df is df
    table_dir = utils.get_table_dir("mycol")
    assert not any(f.endswith(".parquet") for f in os.listdir(table_dir))


def test_process_accumulative_df_delta_phase_flushes_on_batch_size(monkeypatch):
    pushed = []
    written = []
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: pushed.append(path))
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype: written.append((name, obj)),
    )
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: 4)

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0] * 5, "_id": [1, 2, 3, 4, 5]})
    logger = logging.getLogger("test")

    result_df, last_sync_time = listening.process_accumulative_df(
        df, "mycol", "Y", time.time(), 9999, {"tok": 1}, logger,
    )

    assert result_df is None
    assert len(pushed) == 1

    resume_writes = [w for w in written if w[0] == DELTA_SYNC_RESUME_TOKEN_FILE_NAME]
    assert resume_writes == [(DELTA_SYNC_RESUME_TOKEN_FILE_NAME, {"tok": 1})]

    counter_writes = [w for w in written if w[0] == LAST_PARQUET_FILE_NUMBER]
    assert counter_writes == [(LAST_PARQUET_FILE_NUMBER, 5)]


def test_process_accumulative_df_delta_phase_flushes_on_time_threshold(monkeypatch):
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: None)
    monkeypatch.setattr(listening, "write_to_file", lambda *a, **k: None)
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: 0)

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0], "_id": [1]})
    logger = logging.getLogger("test")

    result_df, last_sync_time = listening.process_accumulative_df(
        df, "mycol", "Y", time.time() - 100, 1, {"tok": 1}, logger,
    )

    assert result_df is None


def test_process_accumulative_df_delta_phase_does_not_flush_below_batch_and_time(monkeypatch):
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: None)
    monkeypatch.setattr(listening, "write_to_file", lambda *a, **k: None)

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0], "_id": [1]})
    logger = logging.getLogger("test")

    result_df, last_sync_time = listening.process_accumulative_df(
        df, "mycol", "Y", time.time(), 9999, {"tok": 1}, logger,
    )

    assert result_df is df


def test_process_accumulative_df_defaults_parquet_counter_to_0_when_missing(monkeypatch):
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: None)
    written = []
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype: written.append((name, obj)),
    )
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: None)

    df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0], "_id": [1]})
    logger = logging.getLogger("test")

    listening.process_accumulative_df(
        df, "mycol", "Y", time.time(), 9999, {"tok": 1}, logger, force_flush=True,
    )

    counter_writes = [obj for name, obj in written if name == LAST_PARQUET_FILE_NUMBER]
    assert counter_writes == [1]


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
        lambda obj, table, name, ftype: written.append((name, obj)),
    )
    monkeypatch.setattr(listening, "push_table_metadata_files", lambda table: None)
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)

    schemas.init_table_schema_to_mem("mycol", {})

    accumulative_df = pd.DataFrame({ROW_MARKER_COLUMN_NAME: [0], "_id": [1]})
    offending_doc = {"_id": 2, "new_field": "value"}
    logger = logging.getLogger("test")

    new_df, new_last_sync_time = listening.__bump_schema_version(
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
    # versioned directory.
    resume_writes = [w for w in written if w[0] == DELTA_SYNC_RESUME_TOKEN_FILE_NAME]
    assert resume_writes
    assert all(w == (DELTA_SYNC_RESUME_TOKEN_FILE_NAME, {"tok": 1}) for w in resume_writes)


def test_bump_schema_version_skips_flush_when_no_accumulated_batch(monkeypatch):
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: 0)
    pushed = []
    monkeypatch.setattr(listening, "push_file_to_lz", lambda path, table: pushed.append(path))
    monkeypatch.setattr(listening, "write_to_file", lambda *a, **k: None)
    monkeypatch.setattr(listening, "push_table_metadata_files", lambda table: None)
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)

    schemas.init_table_schema_to_mem("mycol", {})

    new_df, new_last_sync_time = listening.__bump_schema_version(
        "mycol", None, {"tok": 1}, "Y", time.time(), 9999, {"_id": 1}, logging.getLogger("test"),
    )

    assert utils.get_schema_version("mycol") == 2
    # No accumulated batch, so nothing should have been pushed by the pre-bump flush.
    assert pushed == []
    assert new_df[ROW_MARKER_COLUMN_NAME].iloc[0] == CHANGE_STREAM_OPERATION_MAP["insert"]


# ===========================================================================
# listening() - main loop error handling. Driven via a BaseException sentinel
# (not caught by the pymongo-specific except clause) to deterministically
# stop the `while True` loop after a bounded number of iterations.
# ===========================================================================

class _StopLoop(BaseException):
    """Sentinel used to break out of listening()'s infinite loop in tests."""


def _install_fake_collection(monkeypatch, collection):
    class _FakeDb(dict):
        def __getitem__(self, name):
            return collection

    class _FakeClient:
        def __getitem__(self, name):
            return _FakeDb()

    monkeypatch.setattr(listening.pymongo, "MongoClient", lambda *a, **k: _FakeClient())


def _patch_listening_startup(monkeypatch, resume_token=None):
    monkeypatch.setattr(listening, "init_sync", lambda collection_name: None)
    monkeypatch.setattr(
        listening, "read_from_file",
        lambda table, name, ftype: resume_token,
    )


def test_listening_non_resumable_error_clears_resume_token_and_reopens(monkeypatch, fake_collection):
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
        lambda obj, table, name, ftype: written.append((name, obj)),
    )
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    # Reopened the stream (2 calls) after the non-resumable error.
    assert calls["n"] == 2
    resume_clears = [obj for name, obj in written if name == DELTA_SYNC_RESUME_TOKEN_FILE_NAME]
    assert resume_clears == [None]


def test_listening_resumable_error_keeps_resume_token_and_retries(monkeypatch, fake_collection):
    calls = {"n": 0}

    def stream_factory(**kwargs):
        calls["n"] += 1
        if calls["n"] <= 2:
            # A resumable error (no code 286 / NonResumableChangeStreamError label).
            raise pymongo.errors.OperationFailure("transient blip", code=None)
        raise _StopLoop()

    collection = fake_collection(stream_factory)
    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch, resume_token={"tok": "still-good"})

    written = []
    monkeypatch.setattr(
        listening, "write_to_file",
        lambda obj, table, name, ftype: written.append((name, obj)),
    )
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    # Retried 3 times total (2 failures + the call that finally raises _StopLoop).
    assert calls["n"] == 3
    # Resumable errors must never clear the resume token/checkpoint.
    assert written == []


def test_listening_processes_insert_change_with_standard_row_marker(monkeypatch, fake_change_stream):
    change = {
        "operationType": "insert",
        "fullDocument": {"_id": 123, "value": "hello"},
        "_id": {"_data": "fake-resume-token-blob"},
    }
    stream = fake_change_stream(events=[change], then_raise=_StopLoop())
    collection = type("C", (), {"watch": lambda self, **kwargs: stream})()

    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch, resume_token=None)
    monkeypatch.setattr(
        listening, "read_from_file",
        lambda table, name, ftype: "Y" if "init_sync_status" in name else None,
    )
    monkeypatch.setattr(listening.schema_utils, "process_dataframe", lambda table, df: None)
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    captured = {}

    def spy_process_accumulative_df(
        accumulative_df, collection_name, init_sync_stat_flag, last_sync_time,
        time_threshold_in_sec, resume_token, logger, force_flush=False,
    ):
        captured["df"] = accumulative_df.copy()
        return accumulative_df, last_sync_time

    monkeypatch.setattr(listening, "process_accumulative_df", spy_process_accumulative_df)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    assert captured["df"][ROW_MARKER_COLUMN_NAME].iloc[0] == CHANGE_STREAM_OPERATION_MAP["insert"]


def test_listening_uses_upsert_row_marker_while_init_sync_incomplete(monkeypatch, fake_change_stream):
    change = {
        "operationType": "insert",
        "fullDocument": {"_id": 123, "value": "hello"},
        "_id": {"_data": "fake-resume-token-blob"},
    }
    stream = fake_change_stream(events=[change], then_raise=_StopLoop())
    collection = type("C", (), {"watch": lambda self, **kwargs: stream})()

    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch, resume_token=None)
    # init_sync_status file never reports "Y" -> still initializing.
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: None)
    monkeypatch.setattr(listening.schema_utils, "process_dataframe", lambda table, df: None)
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    captured = {}

    def spy_process_accumulative_df(
        accumulative_df, collection_name, init_sync_stat_flag, last_sync_time,
        time_threshold_in_sec, resume_token, logger, force_flush=False,
    ):
        captured["df"] = accumulative_df.copy()
        return accumulative_df, last_sync_time

    monkeypatch.setattr(listening, "process_accumulative_df", spy_process_accumulative_df)

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    assert captured["df"][ROW_MARKER_COLUMN_NAME].iloc[0] == CHANGE_STREAM_OPERATION_MAP_WHEN_INIT["insert"]


def test_listening_unsupported_operation_type_is_skipped(monkeypatch, fake_change_stream):
    change = {
        "operationType": "rename",  # not in CHANGE_STREAM_OPERATION_MAP
        "fullDocument": {"_id": 123},
        "_id": {"_data": "fake-resume-token-blob"},
    }
    stream = fake_change_stream(events=[change], then_raise=_StopLoop())
    collection = type("C", (), {"watch": lambda self, **kwargs: stream})()

    _install_fake_collection(monkeypatch, collection)
    _patch_listening_startup(monkeypatch, resume_token=None)
    monkeypatch.setattr(listening, "read_from_file", lambda table, name, ftype: None)
    monkeypatch.setattr(listening.time, "sleep", lambda secs: None)

    calls = []
    monkeypatch.setattr(
        listening, "process_accumulative_df",
        lambda *a, **k: calls.append(a) or (None, None),
    )

    with pytest.raises(_StopLoop):
        listening.listening("mycol")

    # The unsupported operation must never reach process_accumulative_df.
    assert calls == []
