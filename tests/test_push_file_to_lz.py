import os

import pytest
import requests

import push_file_to_lz


# ---------------------------------------------------------------------------
# __patch_file - regression coverage for the corruption root-cause fix:
# a non-2xx response at create/append must raise (via raise_for_status) and
# must never let a bad/partial upload get promoted via rename.
# ---------------------------------------------------------------------------

def test_patch_file_success_calls_create_append_rename_in_order(monkeypatch, tmp_path, make_response):
    calls = []
    monkeypatch.setattr(
        push_file_to_lz.requests, "put",
        lambda url, **kwargs: calls.append(("PUT", url)) or make_response(200),
    )
    monkeypatch.setattr(
        push_file_to_lz.requests, "patch",
        lambda url, **kwargs: calls.append(("PATCH", url)) or make_response(200),
    )

    file_path = tmp_path / "00000000000000000001.parquet"
    file_path.write_bytes(b"parquet-bytes")

    push_file_to_lz.__patch_file("token", str(file_path), "https://lz.example.com/", "mycol")

    assert [c[0] for c in calls] == ["PUT", "PATCH", "PUT"]


def test_patch_file_create_failure_raises_and_never_renames(monkeypatch, tmp_path, make_response):
    calls = []
    monkeypatch.setattr(
        push_file_to_lz.requests, "put",
        lambda url, **kwargs: calls.append(("PUT", url)) or make_response(500),
    )
    monkeypatch.setattr(
        push_file_to_lz.requests, "patch",
        lambda url, **kwargs: calls.append(("PATCH", url)) or make_response(200),
    )

    file_path = tmp_path / "00000000000000000001.parquet"
    file_path.write_bytes(b"parquet-bytes")

    with pytest.raises(requests.HTTPError):
        push_file_to_lz.__patch_file("token", str(file_path), "https://lz.example.com/", "mycol")

    # Only the create PUT happened; append and rename must never be attempted.
    assert len(calls) == 1
    assert calls[0][0] == "PUT"


def test_patch_file_append_failure_raises_and_never_renames(monkeypatch, tmp_path, make_response):
    put_calls = []
    monkeypatch.setattr(
        push_file_to_lz.requests, "put",
        lambda url, **kwargs: put_calls.append(url) or make_response(200),
    )
    monkeypatch.setattr(
        push_file_to_lz.requests, "patch",
        lambda url, **kwargs: make_response(503),
    )

    file_path = tmp_path / "00000000000000000001.parquet"
    file_path.write_bytes(b"parquet-bytes")

    with pytest.raises(requests.HTTPError):
        push_file_to_lz.__patch_file("token", str(file_path), "https://lz.example.com/", "mycol")

    # Only the create PUT happened (1 call); the rename PUT (would be the 2nd
    # PUT) must never occur once the append PATCH failed.
    assert len(put_calls) == 1


# ---------------------------------------------------------------------------
# get_file_from_lz
# ---------------------------------------------------------------------------

def test_get_file_from_lz_returns_status_and_response_on_200(monkeypatch, make_response):
    monkeypatch.setattr(push_file_to_lz, "__get_access_token", lambda *a, **k: "token")
    monkeypatch.setattr(push_file_to_lz.requests, "get", lambda url, headers=None: make_response(200, content=b"data"))

    status, response = push_file_to_lz.get_file_from_lz("mycol", "somefile.pkl")

    assert status == 200
    assert response.content == b"data"


def test_get_file_from_lz_returns_none_none_on_non_200(monkeypatch, make_response):
    monkeypatch.setattr(push_file_to_lz, "__get_access_token", lambda *a, **k: "token")
    monkeypatch.setattr(push_file_to_lz.requests, "get", lambda url, headers=None: make_response(404))

    status, response = push_file_to_lz.get_file_from_lz("mycol", "missing.pkl")

    assert status is None
    assert response is None


# ---------------------------------------------------------------------------
# __clean_up_old_parquet_files
# ---------------------------------------------------------------------------

def test_clean_up_old_parquet_files_deletes_only_older_numeric_files(tmp_path):
    filenames = [
        "00000000000000000001.parquet",
        "00000000000000000002.parquet",
        "00000000000000000003.parquet",
        "notes.txt",
        "Temp_00000001.parquet",
    ]
    for name in filenames:
        (tmp_path / name).write_bytes(b"x")

    current_file = tmp_path / "00000000000000000003.parquet"
    push_file_to_lz.__clean_up_old_parquet_files(str(current_file))

    remaining = set(os.listdir(tmp_path))
    assert remaining == {
        "00000000000000000003.parquet",
        "notes.txt",
        "Temp_00000001.parquet",
    }


def test_clean_up_old_parquet_files_noop_for_prefixed_file(tmp_path):
    filenames = ["00000000000000000001.parquet", "Temp_00000002.parquet"]
    for name in filenames:
        (tmp_path / name).write_bytes(b"x")

    # Current file has a non-numeric stem (prefixed) - clean-up should be a no-op.
    current_file = tmp_path / "Temp_00000002.parquet"
    push_file_to_lz.__clean_up_old_parquet_files(str(current_file))

    remaining = set(os.listdir(tmp_path))
    assert remaining == set(filenames)
