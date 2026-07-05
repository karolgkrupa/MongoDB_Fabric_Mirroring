import os
import pickle

import file_utils
import utils


# ---------------------------------------------------------------------------
# read_from_file
# ---------------------------------------------------------------------------

def test_read_from_file_pickle_happy_path(monkeypatch, make_response):
    payload = pickle.dumps({"a": 1})
    monkeypatch.setattr(
        file_utils, "get_file_from_lz",
        lambda table, name: (200, make_response(200, content=payload)),
    )

    result = file_utils.read_from_file("mycol", "somefile.pkl", file_utils.FileType.PICKLE)

    assert result == {"a": 1}


def test_read_from_file_text_happy_path(monkeypatch, make_response):
    payload = b"hello world"
    monkeypatch.setattr(
        file_utils, "get_file_from_lz",
        lambda table, name: (200, make_response(200, content=payload)),
    )

    result = file_utils.read_from_file("mycol", "somefile.txt", file_utils.FileType.TEXT)

    assert result == "hello world"


def test_read_from_file_missing_returns_none(monkeypatch):
    monkeypatch.setattr(file_utils, "get_file_from_lz", lambda table, name: (404, None))

    result = file_utils.read_from_file("mycol", "missing.pkl", file_utils.FileType.PICKLE)

    assert result is None


def test_read_from_file_corrupted_pickle_self_heals_and_saves_forensic_copy(monkeypatch, make_response):
    corrupted_bytes = b""  # empty content -> EOFError on pickle.loads, matches the original bug report
    monkeypatch.setattr(
        file_utils, "get_file_from_lz",
        lambda table, name: (200, make_response(200, content=corrupted_bytes)),
    )

    result = file_utils.read_from_file("mycol", "_resume_token.pkl", file_utils.FileType.PICKLE)

    assert result is None
    table_dir = utils.get_table_dir("mycol")
    corrupt_dir = os.path.join(table_dir, ".corrupt")
    assert os.path.isdir(corrupt_dir)
    saved_files = os.listdir(corrupt_dir)
    assert len(saved_files) == 1
    assert saved_files[0].startswith("_resume_token.pkl.")
    with open(os.path.join(corrupt_dir, saved_files[0]), "rb") as f:
        assert f.read() == corrupted_bytes


# ---------------------------------------------------------------------------
# read_from_file_with_backup
# ---------------------------------------------------------------------------

def test_read_from_file_with_backup_primary_valid_skips_backup(monkeypatch, make_response):
    calls = []

    def fake_get_file_from_lz(table, name):
        calls.append(name)
        if name == "primary.pkl":
            return 200, make_response(200, content=pickle.dumps("good_token"))
        raise AssertionError("backup should not be queried when primary is valid")

    monkeypatch.setattr(file_utils, "get_file_from_lz", fake_get_file_from_lz)

    value, used_backup, primary_was_corrupted = file_utils.read_from_file_with_backup(
        "mycol", "primary.pkl", "backup.pkl", file_utils.FileType.PICKLE
    )

    assert value == "good_token"
    assert used_backup is False
    assert primary_was_corrupted is False
    assert calls == ["primary.pkl"]


def test_read_from_file_with_backup_falls_back_when_primary_corrupted(monkeypatch, make_response):
    def fake_get_file_from_lz(table, name):
        if name == "primary.pkl":
            return 200, make_response(200, content=b"")  # corrupted (EOFError)
        if name == "backup.pkl":
            return 200, make_response(200, content=pickle.dumps("backup_token"))
        return 404, None

    monkeypatch.setattr(file_utils, "get_file_from_lz", fake_get_file_from_lz)

    value, used_backup, primary_was_corrupted = file_utils.read_from_file_with_backup(
        "mycol", "primary.pkl", "backup.pkl", file_utils.FileType.PICKLE
    )

    assert value == "backup_token"
    assert used_backup is True
    assert primary_was_corrupted is True


def test_read_from_file_with_backup_both_corrupted_returns_none_and_flags_corruption(monkeypatch, make_response):
    def fake_get_file_from_lz(table, name):
        return 200, make_response(200, content=b"")  # corrupted for both primary and backup

    monkeypatch.setattr(file_utils, "get_file_from_lz", fake_get_file_from_lz)

    value, used_backup, primary_was_corrupted = file_utils.read_from_file_with_backup(
        "mycol", "primary.pkl", "backup.pkl", file_utils.FileType.PICKLE
    )

    assert value is None
    assert used_backup is False
    assert primary_was_corrupted is True


def test_read_from_file_with_backup_first_run_neither_file_exists(monkeypatch):
    monkeypatch.setattr(file_utils, "get_file_from_lz", lambda table, name: (404, None))

    value, used_backup, primary_was_corrupted = file_utils.read_from_file_with_backup(
        "mycol", "primary.pkl", "backup.pkl", file_utils.FileType.PICKLE
    )

    assert value is None
    assert used_backup is False
    # Missing (never written yet) is not the same as corrupted - shouldn't be
    # surfaced as a data-continuity risk.
    assert primary_was_corrupted is False


# ---------------------------------------------------------------------------
# write_to_file (with backup_file_name)
# ---------------------------------------------------------------------------

def test_write_to_file_backs_up_prior_value_before_overwriting_primary(monkeypatch):
    pushed_paths = []
    monkeypatch.setattr(file_utils, "push_file_to_lz", lambda path, table: pushed_paths.append(path))

    old_payload = pickle.dumps("old_token")

    def fake_get_file_from_lz(table, name):
        if name == "primary.pkl":
            return 200, type("R", (), {"content": old_payload})()
        return 404, None

    monkeypatch.setattr(file_utils, "get_file_from_lz", fake_get_file_from_lz)

    file_utils.write_to_file(
        "new_token", "mycol", "primary.pkl", file_utils.FileType.PICKLE, backup_file_name="backup.pkl"
    )

    table_dir = utils.get_table_dir("mycol")
    with open(os.path.join(table_dir, "backup.pkl"), "rb") as f:
        assert pickle.load(f) == "old_token"
    with open(os.path.join(table_dir, "primary.pkl"), "rb") as f:
        assert pickle.load(f) == "new_token"
    # Backup must be written (and pushed) before the primary is overwritten.
    assert pushed_paths == [
        os.path.join(table_dir, "backup.pkl"),
        os.path.join(table_dir, "primary.pkl"),
    ]


def test_write_to_file_first_ever_write_skips_backup(monkeypatch):
    pushed_paths = []
    monkeypatch.setattr(file_utils, "push_file_to_lz", lambda path, table: pushed_paths.append(path))
    monkeypatch.setattr(file_utils, "get_file_from_lz", lambda table, name: (404, None))

    file_utils.write_to_file(
        "first_token", "mycol", "primary.pkl", file_utils.FileType.PICKLE, backup_file_name="backup.pkl"
    )

    table_dir = utils.get_table_dir("mycol")
    assert not os.path.exists(os.path.join(table_dir, "backup.pkl"))
    with open(os.path.join(table_dir, "primary.pkl"), "rb") as f:
        assert pickle.load(f) == "first_token"
    assert pushed_paths == [os.path.join(table_dir, "primary.pkl")]


def test_write_to_file_backup_read_failure_does_not_block_primary_write(monkeypatch):
    pushed_paths = []
    monkeypatch.setattr(file_utils, "push_file_to_lz", lambda path, table: pushed_paths.append(path))

    def raising_get_file_from_lz(table, name):
        raise RuntimeError("network blip")

    monkeypatch.setattr(file_utils, "get_file_from_lz", raising_get_file_from_lz)

    file_utils.write_to_file(
        "new_token", "mycol", "primary.pkl", file_utils.FileType.PICKLE, backup_file_name="backup.pkl"
    )

    table_dir = utils.get_table_dir("mycol")
    assert not os.path.exists(os.path.join(table_dir, "backup.pkl"))
    with open(os.path.join(table_dir, "primary.pkl"), "rb") as f:
        assert pickle.load(f) == "new_token"
    assert pushed_paths == [os.path.join(table_dir, "primary.pkl")]


# ---------------------------------------------------------------------------
# append_to_file / delete_file
# ---------------------------------------------------------------------------

def test_append_to_file_text_creates_header_once_then_appends():
    file_utils.append_to_file("first line", "mycol", "log.txt", file_utils.FileType.TEXT)
    file_utils.append_to_file("second line", "mycol", "log.txt", file_utils.FileType.TEXT)

    table_dir = utils.get_table_dir("mycol")
    with open(os.path.join(table_dir, "log.txt")) as f:
        content = f.read()

    assert content.count("Column Name") == 1
    assert "first line" in content
    assert "second line" in content


def test_delete_file_removes_local_file_and_lz_copy(monkeypatch):
    table_dir = utils.get_table_dir("mycol")
    local_path = os.path.join(table_dir, "somefile.pkl")
    with open(local_path, "wb") as f:
        f.write(b"data")

    deleted = []
    monkeypatch.setattr(file_utils, "delete_file_from_lz", lambda table, name: deleted.append((table, name)))

    file_utils.delete_file("mycol", "somefile.pkl")

    assert not os.path.exists(local_path)
    assert deleted == [("mycol", "somefile.pkl")]
