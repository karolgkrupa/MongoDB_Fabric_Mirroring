import os
import pickle

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
# get_schema_version - including corruption self-heal
# ---------------------------------------------------------------------------

def test_get_schema_version_defaults_to_1_when_no_file_exists():
    assert utils.get_schema_version("brand_new_collection") == 1


def test_get_schema_version_round_trips_through_set_schema_version():
    utils.set_schema_version("mycol", 4)
    assert utils.get_schema_version("mycol") == 4


def test_get_schema_version_corrupted_file_defaults_to_1():
    path = utils._schema_version_file_path("mycol")
    with open(path, "wb") as f:
        f.write(b"")  # empty -> EOFError on unpickling

    assert utils.get_schema_version("mycol") == 1


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
    table_dir = utils.get_table_dir("mycol")
    # Pre-create a couple of temp parquet files with the same prefix.
    first_path = utils.get_temp_parquet_full_path_filename("mycol", prefix="Temp_")
    open(first_path, "wb").close()
    second_num_path = utils.get_temp_parquet_full_path_filename("mycol", prefix="Temp_")
    open(second_num_path, "wb").close()

    third_path = utils.get_temp_parquet_full_path_filename("mycol", prefix="Temp_")
    stem = os.path.splitext(os.path.basename(third_path))[0].removeprefix("Temp_")
    assert int(stem) == 3


# ---------------------------------------------------------------------------
# get_last_parquet_file_num_from_existing_files
# ---------------------------------------------------------------------------

def test_get_last_parquet_file_num_from_existing_files_empty_dir_returns_0():
    assert utils.get_last_parquet_file_num_from_existing_files("mycol") == 0


def test_get_last_parquet_file_num_from_existing_files_returns_max_numbered_file():
    table_dir = utils.get_table_dir("mycol")
    for num in (1, 5, 3):
        filename = str(num).zfill(FILE_NAME_LENGTH) + ".parquet"
        open(os.path.join(table_dir, filename), "wb").close()
    # Non-numeric / non-parquet files must be ignored.
    open(os.path.join(table_dir, "notes.txt"), "wb").close()
    open(os.path.join(table_dir, "Temp_00000000000000000099.parquet"), "wb").close()

    assert utils.get_last_parquet_file_num_from_existing_files("mycol") == 5
