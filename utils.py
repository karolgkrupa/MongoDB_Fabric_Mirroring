import os
import pickle
from constants import DATA_FILES_PATH, FILE_NAME_LENGTH, SCHEMA_VERSION_DIR_NAME


def to_string(obj) -> str:
    return str(obj)


def sanitize_table_name(name: str) -> str:
    # Fabric's landing zone silently skips folders containing '.', so map dotted
    # MongoDB collection names to a Fabric-safe form. MongoDB queries continue to
    # use the original (un-sanitized) name.
    return name.replace(".", "_")


def _schema_version_file_path(collection_name: str) -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    versions_dir = os.path.join(current_dir, DATA_FILES_PATH, SCHEMA_VERSION_DIR_NAME)
    os.makedirs(versions_dir, exist_ok=True)
    return os.path.join(versions_dir, sanitize_table_name(collection_name) + ".pkl")


def get_schema_version(collection_name: str) -> int:
    path = _schema_version_file_path(collection_name)
    if not os.path.exists(path):
        return 1
    with open(path, "rb") as f:
        return pickle.load(f)


def set_schema_version(collection_name: str, version: int) -> None:
    path = _schema_version_file_path(collection_name)
    with open(path, "wb") as f:
        pickle.dump(version, f)


def get_effective_table_name(collection_name: str) -> str:
    sanitized = sanitize_table_name(collection_name)
    version = get_schema_version(collection_name)
    if version <= 1:
        return sanitized
    return f"{sanitized}_v{version}"


def get_table_dir(table_name: str) -> str:
    current_dir = os.path.dirname(os.path.abspath(__file__))
    effective_name = get_effective_table_name(table_name)
    table_dir = os.path.join(current_dir, DATA_FILES_PATH, effective_name + os.sep)
    os.makedirs(table_dir, exist_ok=True)
    return table_dir
#changes to get next parquet file num based on the last parquet from LZ, it will pass 0 if first file
def get_parquet_full_path_filename(table_name: str, parquet_filename_int_list: int, prefix: str = "") -> str:
    table_dir = get_table_dir(table_name)
    # parquet_filename_int_list = [
    #     int(os.path.splitext(filename)[0].removeprefix(prefix))
    #     for filename in os.listdir(table_dir)
    #     if os.path.splitext(filename)[1] == ".parquet"
    #     and os.path.splitext(filename)[0].removeprefix(prefix).isnumeric()
    # ]
    #if parquet_filename_int_list:
    #    return os.path.join(table_dir, prefix + __num_to_filename(max(parquet_filename_int_list) + 1))
    return os.path.join(table_dir, prefix + __num_to_filename(parquet_filename_int_list + 1))
    # else:
    #     return os.path.join(table_dir, prefix + __num_to_filename(1))
    
#as temp files will be stored in local only, kept the original logic a is
def get_temp_parquet_full_path_filename(table_name: str, prefix: str = "") -> str:
    table_dir = get_table_dir(table_name)
    tmp_parquet_filename_int_list = [
        int(os.path.splitext(filename)[0].removeprefix(prefix))
        for filename in os.listdir(table_dir)
        if os.path.splitext(filename)[1] == ".parquet"
        and os.path.splitext(filename)[0].removeprefix(prefix).isnumeric()
    ]
    if tmp_parquet_filename_int_list:
        return os.path.join(table_dir, prefix + __num_to_filename(max(tmp_parquet_filename_int_list) + 1))
    else:
        return os.path.join(table_dir, prefix + __num_to_filename(1))


def __num_to_filename(num: int) -> str:
    int_to_str = str(num)
    leading_zeros = FILE_NAME_LENGTH - len(int_to_str)
    return "0" * leading_zeros + int_to_str + ".parquet"
