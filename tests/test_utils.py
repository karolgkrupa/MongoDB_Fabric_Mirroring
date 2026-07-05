import os

import utils
from constants import FILE_NAME_LENGTH


# ---------------------------------------------------------------------------
# sanitize_table_name / get_effective_table_name
# ---------------------------------------------------------------------------

def test_sanitize_table_name_replaces_dots():
    assert utils.sanitize_table_name("db.my.collection") == "db_my_collection"
    assert utils.sanitize_table_name("plain_name") == "plain_name"


def test_get_effective_table_name_version_1_returns_sanitized_name():
    assert utils.get_effective_table_name("my.collection") == "my_collection"


def test_get_effective_table_name_bumped_version_appends_suffix():
    utils.set_schema_version("mycol", 3)
    assert utils.get_effective_table_name("mycol") == "mycol_v3"


# ---------------------------------------------------------------------------
# get_schema_version / set_schema_version
# ---------------------------------------------------------------------------

def test_get_schema_version_defaults_to_1_when_no_file_exists():
    assert utils.get_schema_version("brand_new_collection") == 1


def test_get_schema_version_round_trips_through_set_schema_version():
    utils.set_schema_version("mycol", 4)
    assert utils.get_schema_version("mycol") == 4


# ---------------------------------------------------------------------------
# get_table_dir
# ---------------------------------------------------------------------------

def test_get_table_dir_creates_directory():
    table_dir = utils.get_table_dir("mycol")
    assert os.path.isdir(table_dir)


# ---------------------------------------------------------------------------
# get_parquet_full_path_filename / get_temp_parquet_full_path_filename
# ---------------------------------------------------------------------------

def test_get_parquet_full_path_filename_uses_next_number():
    path0 = utils.get_parquet_full_path_filename("mycol", 0)
    path5 = utils.get_parquet_full_path_filename("mycol", 5)

    stem0 = os.path.splitext(os.path.basename(path0))[0]
    stem5 = os.path.splitext(os.path.basename(path5))[0]

    assert path0.endswith(".parquet")
    assert len(stem0) == FILE_NAME_LENGTH
    assert int(stem0) == 1
    assert int(stem5) == 6


def test_get_temp_parquet_full_path_filename_starts_at_1_when_empty():
    path = utils.get_temp_parquet_full_path_filename("mycol", prefix="Temp_")
    filename = os.path.basename(path)
    assert filename.startswith("Temp_")
    stem = os.path.splitext(filename)[0].removeprefix("Temp_")
    assert int(stem) == 1


def test_get_temp_parquet_full_path_filename_continues_from_max_existing():
    # Pre-create a couple of temp parquet files with the same prefix.
    first_path = utils.get_temp_parquet_full_path_filename("mycol", prefix="Temp_")
    open(first_path, "wb").close()
    second_path = utils.get_temp_parquet_full_path_filename("mycol", prefix="Temp_")
    open(second_path, "wb").close()

    third_path = utils.get_temp_parquet_full_path_filename("mycol", prefix="Temp_")
    stem = os.path.splitext(os.path.basename(third_path))[0].removeprefix("Temp_")
    assert int(stem) == 3
