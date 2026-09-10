import os
from datetime import datetime

import bson
import numpy as np
import pandas as pd

import schema_utils
import schemas
import utils
from constants import TYPE_KEY, DTYPE_KEY, CONVERSION_LOG_FILE_NAME

NoneType = type(None)


# ---------------------------------------------------------------------------
# Type-conversion functions - happy paths
# ---------------------------------------------------------------------------

def test_to_string_happy_path():
    assert schema_utils.to_string(123) == "123"
    assert schema_utils.to_string(None) == ""


def test_to_string_routes_list_and_dict_through_json():
    assert schema_utils.to_string([1, 2]) == "[1, 2]"
    assert schema_utils.to_string({"a": 1}) == '{"a": 1}'


def test_to_numpy_int64_happy_path():
    assert schema_utils.to_numpy_int64(5) == np.int64(5)
    assert schema_utils.to_numpy_int64(5.7) == np.int64(5)
    assert schema_utils.to_numpy_int64(None) is None


def test_to_numpy_int64_decimal128():
    assert schema_utils.to_numpy_int64(bson.Decimal128("10")) == np.int64(10)


def test_to_numpy_bool_happy_path():
    assert schema_utils.to_numpy_bool(1) is True
    assert schema_utils.to_numpy_bool(0) is False
    assert schema_utils.to_numpy_bool("true") is True
    assert schema_utils.to_numpy_bool("false") is False
    assert schema_utils.to_numpy_bool("not-a-bool") is None


def test_to_numpy_float64_happy_path():
    assert schema_utils.to_numpy_float64(3) == np.float64(3.0)
    assert schema_utils.to_numpy_float64("3.5") == np.float64(3.5)


def test_to_datetime_iso_passthrough_for_non_date_values():
    assert schema_utils.to_datetime_iso("2020-01-01") == "2020-01-01"
    assert schema_utils.to_datetime_iso(42) == 42


def test_to_datetime_iso_converts_datetime_to_isoformat():
    assert schema_utils.to_datetime_iso(datetime(2020, 1, 1)) == "2020-01-01T00:00:00"


def test_to_json_string_happy_path():
    assert schema_utils.to_json_string({"a": 1}) == '{"a": 1}'
    assert schema_utils.to_json_string([1, 2, {"b": 2}]) == '[1, 2, {"b": 2}]'


def test_to_json_string_handles_objectid():
    oid = bson.ObjectId("507f1f77bcf86cd799439011")
    assert schema_utils.to_json_string({"id": oid}) == '{"id": "507f1f77bcf86cd799439011"}'


# ---------------------------------------------------------------------------
# Type-conversion functions - failure falls back to default + logs conversion
# ---------------------------------------------------------------------------

def test_to_numpy_int64_failure_falls_back_and_appends_conversion_log(monkeypatch):
    monkeypatch.setattr(schema_utils, "current_column_name", "amount", raising=False)
    monkeypatch.setattr(schema_utils, "table_name", "mycol", raising=False)

    result = schema_utils.to_numpy_int64("not-a-number")

    assert result is None
    log_path = os.path.join(utils.get_table_dir("mycol"), CONVERSION_LOG_FILE_NAME)
    assert os.path.exists(log_path)
    with open(log_path) as f:
        content = f.read()
    assert "amount" in content
    assert "not-a-number" in content


def test_to_numpy_float64_failure_falls_back_and_appends_conversion_log(monkeypatch):
    monkeypatch.setattr(schema_utils, "current_column_name", "price", raising=False)
    monkeypatch.setattr(schema_utils, "table_name", "mycol", raising=False)

    result = schema_utils.to_numpy_float64("not-a-float")

    assert result is None
    log_path = os.path.join(utils.get_table_dir("mycol"), CONVERSION_LOG_FILE_NAME)
    assert os.path.exists(log_path)
    with open(log_path) as f:
        content = f.read()
    assert "price" in content


# ---------------------------------------------------------------------------
# _types_compatible matrix
# ---------------------------------------------------------------------------

def test_types_compatible_numeric_family():
    assert schema_utils._types_compatible(int, float)
    assert schema_utils._types_compatible(np.int64, int)
    assert schema_utils._types_compatible(bson.int64.Int64, np.float64)


def test_types_compatible_bool_family():
    assert schema_utils._types_compatible(bool, np.bool_)


def test_types_compatible_datetime_family():
    from datetime import date
    assert schema_utils._types_compatible(datetime, date)
    assert schema_utils._types_compatible(pd.Timestamp, datetime)


def test_types_compatible_void_accepts_any_concrete_type():
    assert schema_utils._types_compatible(schema_utils.VoidType, int)
    assert schema_utils._types_compatible(schema_utils.VoidType, str)
    assert schema_utils._types_compatible(schema_utils.VoidType, list)
    assert schema_utils._types_compatible(schema_utils.VoidType, NoneType)


def test_types_compatible_nonetype_always_compatible():
    assert schema_utils._types_compatible(int, NoneType)
    assert schema_utils._types_compatible(str, NoneType)


def test_types_compatible_str_accepts_list_dict_objectid_binary():
    assert schema_utils._types_compatible(str, list)
    assert schema_utils._types_compatible(str, dict)
    assert schema_utils._types_compatible(str, bson.ObjectId)
    assert schema_utils._types_compatible(str, bson.binary.Binary)


def test_types_compatible_incompatible_types():
    assert not schema_utils._types_compatible(int, str)
    assert not schema_utils._types_compatible(bool, str)


def test_types_compatible_str_is_a_sink():
    # to_string can absorb any value, so a string column never forces a bump
    assert schema_utils._types_compatible(str, int)
    assert schema_utils._types_compatible(str, bool)
    assert schema_utils._types_compatible(str, float)


# ---------------------------------------------------------------------------
# init_column_schema
# ---------------------------------------------------------------------------

def test_init_column_schema_nonetype_becomes_void_object():
    schema = schema_utils.init_column_schema("object", None)
    assert schema[TYPE_KEY] is schema_utils.VoidType
    assert schema[DTYPE_KEY] == "object"


def test_init_column_schema_nan_becomes_void_object():
    schema = schema_utils.init_column_schema("float64", np.nan)
    assert schema[TYPE_KEY] is schema_utils.VoidType
    assert schema[DTYPE_KEY] == "object"


def test_init_column_schema_decimal128_becomes_float():
    schema = schema_utils.init_column_schema("object", bson.Decimal128("1.5"))
    assert schema[TYPE_KEY] == float
    assert schema[DTYPE_KEY] == "float64"


def test_init_column_schema_applies_dtype_conversion_map():
    int_schema = schema_utils.init_column_schema("int64", 5)
    assert int_schema[DTYPE_KEY] == "Int64"
    assert int_schema[TYPE_KEY] == int

    bool_schema = schema_utils.init_column_schema("bool", True)
    assert bool_schema[DTYPE_KEY] == "boolean"

    dt_schema = schema_utils.init_column_schema("datetime64[ns]", datetime(2020, 1, 1))
    assert dt_schema[DTYPE_KEY] == "datetime64[ms]"


# ---------------------------------------------------------------------------
# process_dataframe - schema-change detection
# ---------------------------------------------------------------------------

def test_process_dataframe_new_column_appends_schema(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {})

    df = pd.DataFrame({"_id": [1], "amount": [42]})
    signal = schema_utils.process_dataframe("mycol", df)

    assert signal is None
    schema = schemas.get_table_column_schema("mycol", "amount")
    assert schema is not None
    # pandas stores an int column as numpy.int64, not the plain python int.
    assert schema[TYPE_KEY] == np.int64


def test_process_dataframe_incompatible_type_change_returns_signal(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {"amount": {TYPE_KEY: int, DTYPE_KEY: "int64"}})

    df = pd.DataFrame({"_id": [1], "amount": ["definitely-a-string"]})
    signal = schema_utils.process_dataframe("mycol", df)

    assert isinstance(signal, schema_utils.SchemaChangeSignal)
    assert signal.column_name == "amount"
    assert signal.expected_type == int
    assert signal.actual_type == str


def test_process_dataframe_compatible_type_change_does_not_signal(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {"amount": {TYPE_KEY: int, DTYPE_KEY: "int64"}})

    df = pd.DataFrame({"_id": [1], "amount": [np.int64(42)]})
    signal = schema_utils.process_dataframe("mycol", df)

    assert signal is None


def test_process_dataframe_nan_value_does_not_signal(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {"amount": {TYPE_KEY: int, DTYPE_KEY: "int64"}})

    df = pd.DataFrame({"_id": [1], "amount": [np.nan]})
    signal = schema_utils.process_dataframe("mycol", df)

    assert signal is None


def test_process_dataframe_void_column_promotes_on_first_concrete_value(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem(
        "mycol",
        {"BasFilesUploaded": {TYPE_KEY: schema_utils.VoidType, DTYPE_KEY: "object"}},
    )

    df = pd.DataFrame({"_id": [1], "BasFilesUploaded": [["a.pdf", "b.pdf"]]})
    signal = schema_utils.process_dataframe("mycol", df)

    assert signal is None
    schema = schemas.get_table_column_schema("mycol", "BasFilesUploaded")
    # lists are stored as str in the internal schema
    assert schema[TYPE_KEY] == str


def test_process_dataframe_void_sibling_fields_do_not_ping_pong(monkeypatch):
    """Re-seed style: one sparse array field empty, the other a list — then swap.
    Neither should raise a schema-change signal once empty fields are Void."""
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {})

    doc_a = pd.DataFrame(
        {"_id": [1], "AnalysedIbans": [["PL123"]], "BasFilesUploaded": [np.nan]}
    )
    assert schema_utils.process_dataframe("mycol", doc_a) is None
    assert schemas.get_table_column_schema("mycol", "AnalysedIbans")[TYPE_KEY] == str
    assert (
        schemas.get_table_column_schema("mycol", "BasFilesUploaded")[TYPE_KEY]
        is schema_utils.VoidType
    )

    doc_b = pd.DataFrame(
        {"_id": [2], "AnalysedIbans": [np.nan], "BasFilesUploaded": [[{"name": "x"}]]}
    )
    assert schema_utils.process_dataframe("mycol", doc_b) is None
    assert schemas.get_table_column_schema("mycol", "BasFilesUploaded")[TYPE_KEY] == str


def test_process_dataframe_uses_renamed_column_schema(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {"Original_Col": {TYPE_KEY: str, DTYPE_KEY: "object"}})
    schemas.init_column_renaming("mycol", {"Original Col": "Original_Col"})

    df = pd.DataFrame({"_id": [1], "Original Col": ["hello"]})
    signal = schema_utils.process_dataframe("mycol", df)

    assert signal is None
    assert "Original_Col" in df.columns
    assert "Original Col" not in df.columns


# ---------------------------------------------------------------------------
# Regressions for Fabric SchemaMergeFailure (column type flipping between files)
# ---------------------------------------------------------------------------

def _parquet_types(df):
    import io
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    buf.seek(0)
    return {f.name: str(f.type) for f in pq.read_schema(buf) if f.name != "_id"}


def test_init_column_schema_nullable_scalars_ignore_object_column_dtype():
    # a bool/int column containing a null is "object" in pandas
    assert schema_utils.init_column_schema("object", True)[DTYPE_KEY] == "boolean"
    assert schema_utils.init_column_schema("object", 5)[DTYPE_KEY] == "Int64"


def test_process_dataframe_suppressed_signal_coerces_and_continues(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem(
        "mycol",
        {
            "flag": {TYPE_KEY: bool, DTYPE_KEY: "boolean"},
            "later": {TYPE_KEY: bool, DTYPE_KEY: "boolean"},
        },
    )

    df = pd.DataFrame(
        {"_id": [1, 2], "flag": [1, 0], "later": pd.Series([True, None], dtype=object)}
    )
    signal = schema_utils.process_dataframe("mycol", df, signal_type_changes=False)

    assert signal is None
    assert df["flag"].tolist() == [True, False]
    # the column after the offending one must still be converted
    assert str(df["later"].dtype) == "boolean"
    assert _parquet_types(df) == {"flag": "bool", "later": "bool"}


def test_process_dataframe_str_column_absorbs_int(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {"Hash": {TYPE_KEY: str, DTYPE_KEY: "object"}})

    df = pd.DataFrame({"_id": [1, 2], "Hash": [12345, 999]})
    signal = schema_utils.process_dataframe("mycol", df)

    assert signal is None
    assert df["Hash"].tolist() == ["12345", "999"]


def test_prepare_df_for_parquet_drops_void_and_restores_schema_dtype(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem(
        "mycol",
        {
            "OfferChosen": {TYPE_KEY: schema_utils.VoidType, DTYPE_KEY: "object"},
            "IsComplete": {TYPE_KEY: bool, DTYPE_KEY: "boolean"},
        },
    )
    # what pd.concat of separately processed CDC rows can produce
    df = pd.DataFrame(
        {
            "_id": pd.Series([1, 2], dtype=object),
            "OfferChosen": [None, None],
            "IsComplete": pd.Series([True, None], dtype=object),
        }
    )

    result = schema_utils.prepare_df_for_parquet("mycol", df)

    assert result is df  # modified in place
    assert "OfferChosen" not in df.columns
    assert df["_id"].tolist() == [1, 2]  # _id is never stringified
    assert _parquet_types(df) == {"IsComplete": "bool"}
