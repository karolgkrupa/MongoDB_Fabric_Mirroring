import schemas
from constants import INTERNAL_SCHEMA_FILE_NAME


def test_init_table_schema_sets_schema_and_persists(monkeypatch):
    written = []
    monkeypatch.setattr(
        schemas, "write_to_file",
        lambda obj, table, name, ftype: written.append((table, name, obj)),
    )

    schema = {"col1": {"type": str, "dtype": "object"}}
    schemas.init_table_schema("mycol", schema)

    assert schemas.get_table_schema("mycol") == schema
    assert written == [("mycol", INTERNAL_SCHEMA_FILE_NAME, schema)]


def test_init_table_schema_to_mem_does_not_persist(monkeypatch):
    written = []
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: written.append(a))

    schema = {"col1": {"type": str, "dtype": "object"}}
    schemas.init_table_schema_to_mem("mycol", schema)

    assert schemas.get_table_schema("mycol") == schema
    assert written == []


def test_append_schema_column_appends_and_persists(monkeypatch):
    written = []
    monkeypatch.setattr(schemas, "write_to_file", lambda obj, table, name, ftype: written.append(obj))
    schemas.init_table_schema_to_mem("mycol", {})

    schemas.append_schema_column("mycol", "new_col", {"type": int, "dtype": "int64"})

    assert schemas.get_table_column_schema("mycol", "new_col") == {"type": int, "dtype": "int64"}
    assert written == [{"new_col": {"type": int, "dtype": "int64"}}]


def test_add_and_find_column_renaming(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {})

    schemas.add_column_renaming("mycol", "Original Col", "Original_Col")

    assert schemas.find_column_renaming("mycol", "Original Col") == "Original_Col"
    assert schemas.find_column_renaming("mycol", "unknown") is None


def test_reset_table_schema_clears_state_but_keeps_lock_for_reseeding(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {"col1": {}})
    schemas.add_column_renaming("mycol", "Col 1", "Col_1")

    schemas.reset_table_schema("mycol")

    assert schemas.get_table_schema("mycol") is None
    assert schemas.get_table_column_renaming("mycol") is None

    # Lock must still exist so re-seeding via append_schema_column/add_column_renaming
    # doesn't KeyError after a schema-version bump resets state mid-run.
    schemas.append_schema_column("mycol", "col2", {"type": str})
    assert schemas.get_table_column_schema("mycol", "col2") == {"type": str}
