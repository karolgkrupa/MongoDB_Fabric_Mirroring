import os
import pickle
import logging
import time
from enum import Enum
from typing import Any, Optional, Tuple
import utils
from push_file_to_lz import push_file_to_lz, get_file_from_lz, delete_file_from_lz

logger = logging.getLogger(__name__)


class FileType(Enum):
    PICKLE = "pickle"
    TEXT = "text"


FILETYPE_TO_READ_MODE_MAP = {
    FileType.PICKLE: "rb",
    FileType.TEXT: "r",
}

FILETYPE_TO_WRITE_MODE_MAP = {
    FileType.PICKLE: "wb",
    FileType.TEXT: "w",
}

# Exceptions that mean the checkpoint bytes exist but can't be deserialized
# (empty/partial/garbage content), as opposed to the file genuinely being
# absent. These are self-healed: logged loudly, preserved for forensics, and
# treated like a missing file so the caller's existing "if not X" fallback
# logic (recompute/restart/re-derive) kicks in instead of crashing.
CORRUPTION_EXCEPTIONS = (
    EOFError,
    pickle.UnpicklingError,
    AttributeError,
    ImportError,
    ModuleNotFoundError,
    IndexError,
)


def _save_corrupted_copy(table_name: str, file_name: str, raw_content: bytes) -> Optional[str]:
    """Best-effort preservation of corrupted checkpoint bytes for later forensics.
    Must never raise, since a failure here must not block self-healing."""
    try:
        table_path = utils.get_table_dir(table_name)
        corrupt_dir = os.path.join(table_path, ".corrupt")
        os.makedirs(corrupt_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%dT%H%M%S")
        corrupt_path = os.path.join(corrupt_dir, f"{file_name}.{timestamp}.bad")
        with open(corrupt_path, "wb") as f:
            f.write(raw_content or b"")
        return corrupt_path
    except Exception as e:
        logger.warning(
            f"Failed to preserve corrupted checkpoint copy for table={table_name} file={file_name}: {e}"
        )
        return None


def _deserialize(file_type: FileType, raw_content: bytes):
    """Raises one of CORRUPTION_EXCEPTIONS if raw_content can't be deserialized."""
    if file_type == FileType.PICKLE:
        obj = pickle.loads(raw_content)
        # Check if the result is itself a pickled object (nested)
        if isinstance(obj, bytes):
            obj = pickle.loads(obj)
        return obj
    elif file_type == FileType.TEXT:
        return raw_content.decode('utf-8')
    else:
        return None


def read_from_file(table_name: str, file_name: str, file_type: FileType):
    # always read from LZ first
    response_status_code, file_content = get_file_from_lz(table_name, file_name)
    if response_status_code != 200:
        return None
    try:
        return _deserialize(file_type, file_content.content)
    except CORRUPTION_EXCEPTIONS as e:
        corrupt_path = _save_corrupted_copy(table_name, file_name, file_content.content)
        content_length = len(file_content.content) if file_content.content is not None else 0
        logger.error(
            f"Corrupted checkpoint detected for table={table_name} file={file_name}: "
            f"{type(e).__name__}: {e}. content_length={content_length}. "
            f"Treating as missing so the caller can self-heal. Corrupted copy saved to: {corrupt_path}"
        )
        return None


def read_from_file_with_backup(
    table_name: str,
    primary_file_name: str,
    backup_file_name: str,
    file_type: FileType,
) -> Tuple[Any, bool, bool]:
    """Read a checkpoint, falling back to a backup copy if the primary exists but
    is corrupted (as opposed to genuinely missing, in which case there's nothing
    to recover from a backup and callers should fall back to their own default).

    Returns (value, used_backup, primary_was_corrupted).
    - value is None if neither the primary nor the backup could be read.
    - used_backup is True if the returned value came from the backup file.
    - primary_was_corrupted is True only when the primary file was present but
      failed to deserialize (as opposed to simply not existing yet), so callers
      can distinguish a legitimate first run (nothing to recover, not an error)
      from an actual checkpoint loss worth surfacing loudly.
    """
    primary_was_corrupted = False
    status_code, content = get_file_from_lz(table_name, primary_file_name)
    if status_code == 200:
        try:
            return _deserialize(file_type, content.content), False, False
        except CORRUPTION_EXCEPTIONS as e:
            primary_was_corrupted = True
            corrupt_path = _save_corrupted_copy(table_name, primary_file_name, content.content)
            logger.error(
                f"Corrupted primary checkpoint for table={table_name} file={primary_file_name}: "
                f"{type(e).__name__}: {e}. Corrupted copy saved to: {corrupt_path}. "
                f"Attempting to recover from backup file={backup_file_name}."
            )

    # Primary is either missing (nothing to do here) or corrupted (try the backup).
    backup_status_code, backup_content = get_file_from_lz(table_name, backup_file_name)
    if backup_status_code == 200:
        try:
            return _deserialize(file_type, backup_content.content), True, primary_was_corrupted
        except CORRUPTION_EXCEPTIONS as e:
            corrupt_path = _save_corrupted_copy(table_name, backup_file_name, backup_content.content)
            logger.error(
                f"Backup checkpoint also corrupted for table={table_name} file={backup_file_name}: "
                f"{type(e).__name__}: {e}. Corrupted copy saved to: {corrupt_path}."
            )

    return None, False, primary_was_corrupted


def _write_local_and_push(obj: Any, table_path: str, table_name: str, file_name: str, file_type: FileType):
    file_full_path = os.path.join(table_path, file_name)
    with open(file_full_path, FILETYPE_TO_WRITE_MODE_MAP.get(file_type, "w")) as file:
        if file_type == FileType.PICKLE:
            pickle.dump(obj, file)
        elif file_type == FileType.TEXT:
            file.write(obj)
    # write to LZ
    push_file_to_lz(file_full_path, table_name)


def write_to_file(
    obj: Any,
    table_name: str,
    file_name: str,
    file_type: FileType,
    backup_file_name: str = None,
):
    table_path = utils.get_table_dir(table_name)
    if backup_file_name:
        # Best-effort: before overwriting the primary, carry its current (still-good)
        # value forward into the backup slot. If the primary can't currently be read
        # (missing or already corrupted), skip updating the backup this cycle so an
        # older-but-valid backup isn't clobbered.
        try:
            current_value = read_from_file(table_name, file_name, file_type)
            if current_value is not None:
                _write_local_and_push(current_value, table_path, table_name, backup_file_name, file_type)
        except Exception as e:
            logger.warning(
                f"Skipping backup update for table={table_name} file={backup_file_name} "
                f"due to error reading current primary value: {e}"
            )
    _write_local_and_push(obj, table_path, table_name, file_name, file_type)


def append_to_file(obj: Any, table_name: str, file_name: str, file_type: FileType):
    table_path = utils.get_table_dir(table_name)
    file_full_path = os.path.join(table_path, file_name)
    append_mode = "ab" if file_type == FileType.PICKLE else "a"
    
    # Create file if it doesn't exist
    if not os.path.exists(file_full_path):
        with open(file_full_path, FILETYPE_TO_WRITE_MODE_MAP.get(file_type, "w")) as f:
            f.write(f"\n{'Column Name':<20} | {'Original Value':<20} | {'Converting Value':<20}\n{'-'*70}\n")
    
    with open(file_full_path, append_mode) as file:
        if file_type == FileType.PICKLE:
            pickle.dump(obj, file)
        elif file_type == FileType.TEXT:
            file.write(obj)
    # write to LZ
    # push_file_to_lz(file_full_path, table_name)


def delete_file(table_name: str, file_name: str):
    file_full_path = os.path.join(utils.get_table_dir(table_name), file_name)
    os.remove(file_full_path)
    # delete from LZ
    delete_file_from_lz(table_name, file_name)
