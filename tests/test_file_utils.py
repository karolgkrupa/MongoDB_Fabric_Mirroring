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


def test_read_from_file_nested_pickle_is_unwrapped(monkeypatch, make_response):
    # read_from_file re-unpickles the result if it comes back as raw bytes,
    # covering values that were pickled twice.
    payload = pickle.dumps(pickle.dumps("inner_value"))
    monkeypatch.setattr(
        file_utils, "get_file_from_lz",
        lambda table, name: (200, make_response(200, content=payload)),
    )

    result = file_utils.read_from_file("mycol", "somefile.pkl", file_utils.FileType.PICKLE)

    assert result == "inner_value"


# ---------------------------------------------------------------------------
# write_to_file
# ---------------------------------------------------------------------------

def test_write_to_file_pickle_writes_locally_and_pushes_to_lz(monkeypatch):
    pushed = []
    monkeypatch.setattr(file_utils, "push_file_to_lz", lambda path, table: pushed.append(path))

    file_utils.write_to_file({"a": 1}, "mycol", "somefile.pkl", file_utils.FileType.PICKLE)

    table_dir = utils.get_table_dir("mycol")
    file_path = os.path.join(table_dir, "somefile.pkl")
    with open(file_path, "rb") as f:
        assert pickle.load(f) == {"a": 1}
    assert pushed == [file_path]


def test_write_to_file_text_writes_locally_and_pushes_to_lz(monkeypatch):
    pushed = []
    monkeypatch.setattr(file_utils, "push_file_to_lz", lambda path, table: pushed.append(path))

    file_utils.write_to_file("hello", "mycol", "somefile.txt", file_utils.FileType.TEXT)

    table_dir = utils.get_table_dir("mycol")
    file_path = os.path.join(table_dir, "somefile.txt")
    with open(file_path) as f:
        assert f.read() == "hello"
    assert pushed == [file_path]


# ---------------------------------------------------------------------------
# append_to_file
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


def test_append_to_file_does_not_push_to_lz(monkeypatch):
    pushed = []
    monkeypatch.setattr(file_utils, "push_file_to_lz", lambda path, table: pushed.append(path))

    file_utils.append_to_file("a line", "mycol", "log.txt", file_utils.FileType.TEXT)

    assert pushed == []


# ---------------------------------------------------------------------------
# delete_file
# ---------------------------------------------------------------------------

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
