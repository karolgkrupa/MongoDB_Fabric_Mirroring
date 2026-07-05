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
    assert not schema_utils._types_compatible(str, int)


# ---------------------------------------------------------------------------
# init_column_schema
# ---------------------------------------------------------------------------

def test_init_column_schema_nonetype_becomes_str_object():
    schema = schema_utils.init_column_schema("object", None)
    assert schema[TYPE_KEY] == str
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


def test_process_dataframe_uses_renamed_column_schema(monkeypatch):
    monkeypatch.setattr(schemas, "write_to_file", lambda *a, **k: None)
    schemas.init_table_schema_to_mem("mycol", {"Original_Col": {TYPE_KEY: str, DTYPE_KEY: "object"}})
    schemas.init_column_renaming("mycol", {"Original Col": "Original_Col"})

    df = pd.DataFrame({"_id": [1], "Original Col": ["hello"]})
    signal = schema_utils.process_dataframe("mycol", df)

    assert signal is None
    assert "Original_Col" in df.columns
    assert "Original Col" not in df.columns
