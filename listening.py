import pymongo
import pandas as pd
import time
import logging
import os
import pickle
from threading import Thread
import threading
from pymongo.errors import PyMongoError
from datetime import datetime, timedelta

from constants import (
    ROW_MARKER_COLUMN_NAME,
    CHANGE_STREAM_OPERATION_MAP,
    CHANGE_STREAM_OPERATION_MAP_WHEN_INIT,
    TYPES_TO_CONVERT_TO_STR,
    TEMP_PREFIX_DURING_INIT,
    DATA_FILES_PATH,
    DELTA_SYNC_CACHE_PARQUET_FILE_NAME,
    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
    DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME,
# added the two new files to save the initial sync status and last parquet file number
    INIT_SYNC_STATUS_FILE_NAME,
    LAST_PARQUET_FILE_NUMBER,
    DTYPE_KEY,
    TYPE_KEY,
)
from utils import (
    to_string,
    get_parquet_full_path_filename,
    get_temp_parquet_full_path_filename,
    get_table_dir,
    get_schema_version,
    set_schema_version,
)
from push_file_to_lz import (
    push_file_to_lz,
    push_table_metadata_files,
    get_last_parquet_file_num_from_lz,
)
#from flags import get_init_flag
from init_sync import init_sync
import schemas
import schema_utils
from file_utils import FileType, read_from_file, write_to_file, read_from_file_with_backup

def listening(collection_name: str):
    logger = logging.getLogger(f"{__name__}[{collection_name}]")
    db_name = os.getenv("MONGO_DB_NAME")
    logger.debug(f"db_name={db_name}")
    logger.debug(f"collection={collection_name}")
    # moved listening method so that it is called after the env variables are loaded
    time_threshold_in_sec = float(os.getenv("TIME_THRESHOLD_IN_SEC"))
    # How long a "resumable" change stream error is allowed to keep recurring (with
    # the same stale resume_token) before we give up and treat it like a
    # non-resumable error instead of retrying forever. Default: 5 minutes.
    resumable_error_max_retry_sec = float(os.getenv("RESUMABLE_ERROR_MAX_RETRY_SEC", "300"))
    post_init_flush_done = False

    # table_dir = get_table_dir(collection_name) #Never used
    resume_token, used_backup_resume_token, primary_resume_token_corrupted = read_from_file_with_backup(
        collection_name,
        DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
        DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME,
        FileType.PICKLE,
    )
    # While True, plain "insert" events are persisted with the Upsert row marker
    # instead of Insert, so that any already-processed changes replayed from a
    # backup/stale checkpoint get merged instead of duplicated. Cleared once a
    # fresh resume_token checkpoint is successfully written again.
    recovering_from_backup_token = False
    if resume_token:
        if used_backup_resume_token:
            recovering_from_backup_token = True
            logger.warning(
                "resume_token for collection %s recovered from BACKUP checkpoint (primary was "
                "corrupted): resume_token=%s. Some already-processed changes may be replayed; "
                "they will be upserted (not duplicated) until the next successful checkpoint.",
                collection_name,
                resume_token,
            )
        else:
            logger.info(
                f"interrupted incremental sync detected, continuing with resume_token={resume_token}"
            )
    elif primary_resume_token_corrupted:
        logger.critical(
            "DATA CONTINUITY RISK: resume_token for collection %s could not be recovered from "
            "either the primary or backup checkpoint (both corrupted/unreadable). The change "
            "stream will resume from 'now' - any changes since the last known-good checkpoint "
            "may be skipped. Inspect the corrupted checkpoint copies saved under the collection's "
            ".corrupt/ folder.",
            collection_name,
        )

    #MongoDB connection and data info
    client = pymongo.MongoClient(
        os.getenv("MONGO_CONN_STR"),
        # 0 or None = no driver‑side socket timeout
        socketTimeoutMS=None,
        # (optionally) set a sane connect timeout instead of a read timeout
        connectTimeoutMS=20000,
    )
    db = client[db_name]
    collection = db[collection_name]

    #cursor = collection.watch(full_document="updateLookup", resume_after=resume_token, max_await_time_ms=20000)

    # use df  - enables variable schemas
    # and consistent as resume_token is updated when file is pushed to LZ
    accumulative_df: pd.DataFrame = None
    init_sync_stat_flag = None
    last_sync_time: float | None = None
    # Tracks how long we've been continuously failing with a "resumable" change
    # stream error. None means no active failure streak. Reset on any successful
    # try_next() call (proof the stream is currently healthy).
    resumable_failure_streak_start: float | None = None

    # start init sync after we get cursor from Change Stream
    Thread(target=init_sync, args=(collection_name,)).start()
    logger.info(f"start listening to change stream for collection {collection_name}")
    
    # New main loop logic
    while True:
        # Build watch options each time we open a new stream
        watch_kwargs = dict(
            full_document="updateLookup",
            max_await_time_ms=20000,
        )
        if resume_token:
            watch_kwargs["resume_after"] = resume_token

        try:
            with collection.watch(**watch_kwargs) as stream:
                logger.info(
                    "opened change stream for %s with resume_token=%s",
                    collection_name,
                    resume_token,
                )
                last_action_time = datetime.now()
                # Use try_next so we can flush on time threshold even without new events
                while True:
                    before = time.time()
                    change = stream.try_next()
                    after = time.time()
                    # Forward progress made; any ongoing resumable-error streak is over.
                    resumable_failure_streak_start = None

                    if change is None:
                        if (datetime.now() - last_action_time >= timedelta(minutes=5)):
                            logger.info("no change; try_next() round-trip took %.3fs", after - before)
                            last_action_time = datetime.now()
                        # No new events in this await interval; consider time-based flush
                        if (
                            accumulative_df is not None
                            and init_sync_stat_flag == "Y"
                            and last_sync_time is not None
                        ):
                            accumulative_df, last_sync_time, recovering_from_backup_token = process_accumulative_df(
                                accumulative_df,
                                collection_name,
                                init_sync_stat_flag,
                                last_sync_time,
                                time_threshold_in_sec,
                                resume_token,
                                logger,
                                recovering_from_backup_token,
                            )
                        continue

                    # ---- We have a real change document here ----

                    if init_sync_stat_flag != "Y":
                        init_sync_stat_flag = read_from_file(
                            collection_name,
                            INIT_SYNC_STATUS_FILE_NAME,
                            FileType.PICKLE,
                        )

                    if init_sync_stat_flag == "Y" and not post_init_flush_done:
                        __post_init_flush(collection_name, logger)
                        post_init_flush_done = True

                    logger.debug("original change from Change Stream:")
                    logger.debug(change)

                    operationType = change["operationType"]
                    if operationType not in CHANGE_STREAM_OPERATION_MAP:
                        logger.error("ERROR: unsupported operation found: %s", operationType)
                        continue

                    if operationType == "delete":
                        doc: dict = change["documentKey"]
                    else:
                        doc: dict = change["fullDocument"]

                    df = pd.DataFrame([doc])

                    # Always update resume_token on every processed change
                    resume_token = change["_id"]
                    logger.debug("resume_token: %s", resume_token)

                    # Before init sync finishes we can't bump the schema version, so
                    # coerce incompatible values to the schema type instead.
                    schema_change_signal = schema_utils.process_dataframe(
                        collection_name,
                        df,
                        signal_type_changes=(init_sync_stat_flag == "Y"),
                    )

                    if schema_change_signal is not None and init_sync_stat_flag == "Y":
                        logger.warning(
                            "schema-change signal received for collection %s on column %s "
                            "(expected=%s, actual=%s); bumping schema version",
                            collection_name,
                            schema_change_signal.column_name,
                            schema_change_signal.expected_type,
                            schema_change_signal.actual_type,
                        )
                        accumulative_df, last_sync_time, recovering_from_backup_token = __bump_schema_version(
                            collection_name,
                            accumulative_df,
                            resume_token,
                            init_sync_stat_flag,
                            last_sync_time,
                            time_threshold_in_sec,
                            doc,
                            logger,
                            recovering_from_backup_token,
                        )
                        continue

                    if init_sync_stat_flag != "Y":
                        logger.debug(
                            "collection %s still initializing, use UPSERT instead of INSERT",
                            collection_name,
                        )
                        row_marker_value = CHANGE_STREAM_OPERATION_MAP_WHEN_INIT[
                            operationType
                        ]
                    elif recovering_from_backup_token and operationType == "insert":
                        # Replaying from a backup/stale resume_token may redeliver changes we
                        # already processed. A plain Insert would duplicate the row, so use
                        # Upsert instead until a fresh checkpoint closes the replay window.
                        logger.debug(
                            "collection %s replaying insert while recovering from backup "
                            "resume_token; using UPSERT instead of INSERT",
                            collection_name,
                        )
                        row_marker_value = CHANGE_STREAM_OPERATION_MAP_WHEN_INIT["insert"]
                    else:
                        row_marker_value = CHANGE_STREAM_OPERATION_MAP[operationType]

                    df.insert(0, ROW_MARKER_COLUMN_NAME, [row_marker_value])

                    # Merge into accumulative_df until batch size/time threshold
                    if accumulative_df is not None:
                        accumulative_df = pd.concat(
                            [accumulative_df, df], ignore_index=True
                        )
                        logger.info(
                            "added record to batch; accumulative rows=%d",
                            accumulative_df.shape[0],
                        )
                    else:
                        logger.info("df created")
                        accumulative_df = df
                        last_sync_time = time.time()
                        logger.info(
                            "last_sync_time when first record added: %s", last_sync_time
                        )

                    accumulative_df, last_sync_time, recovering_from_backup_token = process_accumulative_df(
                        accumulative_df,
                        collection_name,
                        init_sync_stat_flag,
                        last_sync_time,
                        time_threshold_in_sec,
                        resume_token,
                        logger,
                        recovering_from_backup_token,
                    )

                # End inner while True

        except (
            pymongo.errors.ConnectionFailure,
            pymongo.errors.CursorNotFound,
            pymongo.errors.OperationFailure,
            pymongo.errors.PyMongoError,
        ) as exc:
            # Detect non-resumable ChangeStreamHistoryLost / stale resume token.
            is_non_resumable = (
                isinstance(exc, pymongo.errors.OperationFailure)
                and (
                    exc.code == 286  # ChangeStreamHistoryLost
                    or exc.has_error_label("NonResumableChangeStreamError")
                )
            )

            if not is_non_resumable:
                # Track how long this "resumable" error has kept recurring without any
                # forward progress. A permanently-stale resume_token (e.g. after a
                # replica-set key rotation) can look resumable but never actually
                # succeed, so retrying forever with the same token would leave the
                # collection stuck indefinitely.
                now = time.time()
                if resumable_failure_streak_start is None:
                    resumable_failure_streak_start = now
                elif now - resumable_failure_streak_start >= resumable_error_max_retry_sec:
                    logger.critical(
                        "DATA CONTINUITY RISK: resumable change stream error for collection %s "
                        "has been recurring for over %.0fs with no forward progress "
                        "(resume_token=%s); escalating to a non-resumable reset instead of "
                        "retrying forever.",
                        collection_name,
                        resumable_error_max_retry_sec,
                        resume_token,
                        exc_info=True,
                    )
                    is_non_resumable = True
                    resumable_failure_streak_start = None

            if is_non_resumable:
                logger.critical(
                    "DATA CONTINUITY RISK: Non-resumable Change Stream error (ChangeStreamHistoryLost) "
                    "for collection %s: %s. Clearing resume token and restarting from latest position - "
                    "any changes between the last known-good checkpoint and now may be skipped.",
                    collection_name,
                    exc,
                    exc_info=True,
                )

                # Drop the bad resume token so the next loop does *not* send resume_after.
                resume_token = None
                # No longer meaningful once we're intentionally resetting to "now".
                recovering_from_backup_token = False

                # Persist that change so a restart doesn't reuse the stale token. Clear the
                # backup too, since resuming from it later would be just as unsafe/stale.
                write_to_file(
                    None,
                    collection_name,
                    DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
                    FileType.PICKLE,
                )
                write_to_file(
                    None,
                    collection_name,
                    DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME,
                    FileType.PICKLE,
                )

                # Optional: clear any in-memory batch since we've lost continuity anyway.
                accumulative_df = None
                last_sync_time = None
            else:
                # Resumable errors: keep the last known resume_token.
                logger.warning(
                    "Resumable change stream error for collection %s: %s; "
                    "will reopen with last resume_token=%s",
                    collection_name,
                    exc,
                    resume_token,
                    exc_info=True,
                )

            # Outer while True will rebuild watch_kwargs and reopen.
            # Slight backoff to avoid tight reconnect loop
            time.sleep(2)
            continue

        except Exception as exc:
            # Last-resort safety net for anything not already handled above (e.g. an
            # HTTP failure raised out of push_file_to_lz via __bump_schema_version, or
            # any other unexpected error). Without this, an uncaught exception here
            # would be handled by Python's *default* thread exception hook, which
            # prints to stderr only and never reaches mirroring.log - silently ending
            # this collection's sync with no trace in the log anyone would check.
            logger.critical(
                "DATA CONTINUITY RISK: unexpected error in listening loop for collection %s: %s. "
                "Thread will keep running and retry rather than dying silently.",
                collection_name,
                exc,
                exc_info=True,
            )
            time.sleep(2)
            continue

        # If we ever exit the inner loop *without* an exception:
        # check stream.alive to see if the server closed the cursor.
        if not stream.alive:
            logger.warning(
                "change stream closed by server for collection %s; reopening with last resume_token=%s",
                collection_name,
                resume_token,
            )
            # Outer while True will reopen
            continue

##>> enhancement to check time elapsed even if no event comes - no waiting indefinitely for a change
def process_accumulative_df(accumulative_df, collection_name, init_sync_stat_flag, last_sync_time, time_threshold_in_sec, resume_token, logger, recovering_from_backup_token=False, force_flush=False):
    if not init_sync_stat_flag == "Y":
        if (accumulative_df is not None
            and (
                force_flush
                or (accumulative_df.shape[0] >= int(os.getenv("DELTA_SYNC_BATCH_SIZE")))
            )
        ):
            try:
                prefix = TEMP_PREFIX_DURING_INIT
                parquet_full_path_filename = get_temp_parquet_full_path_filename(
                    collection_name, prefix=prefix
                )
                logger.info(f"writing TEMP parquet file: {parquet_full_path_filename}")
                schema_utils.prepare_df_for_parquet(collection_name, accumulative_df)
                accumulative_df.to_parquet(parquet_full_path_filename)
                accumulative_df = None
            except Exception as exc:
                # Never let a write failure here silently kill the listening thread -
                # keep the batch in memory so the next check retries the whole write.
                logger.critical(
                    "DATA CONTINUITY RISK: failed to write TEMP parquet file for collection %s: "
                    "%s. Batch retained in memory and will be retried.",
                    collection_name,
                    exc,
                    exc_info=True,
                )
    else:
        if (accumulative_df is not None
        ):
            if(
                force_flush
                or (accumulative_df.shape[0] >= int(os.getenv("DELTA_SYNC_BATCH_SIZE")))
                or ((time.time() - last_sync_time) >= time_threshold_in_sec)
            ):
                try:
                    prefix = ""
                    last_parquet_file_num = read_from_file(
                        collection_name, LAST_PARQUET_FILE_NUMBER, FileType.PICKLE
                    )
                    if not last_parquet_file_num:
                        last_parquet_file_num = get_last_parquet_file_num_from_lz(collection_name)

                    parquet_full_path_filename = get_parquet_full_path_filename(collection_name, last_parquet_file_num)

                    logger.info(f"writing parquet file: {parquet_full_path_filename}")
                    # Align dtypes with the schema, drop all-null Void columns and
                    # stringify any remaining Object column
                    schema_utils.prepare_df_for_parquet(collection_name, accumulative_df)
                    # Write the parquet file
                    accumulative_df.to_parquet(parquet_full_path_filename)

                    push_file_to_lz(parquet_full_path_filename, collection_name)
                #    resume_token = change["_id"]
                    logger.info(f"writing resume_token into file: {resume_token}")
                    write_to_file(
                        resume_token,
                        collection_name,
                        DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
                        FileType.PICKLE,
                        backup_file_name=DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME,
                    )
                    last_parquet_file_num +=  1
                    logger.info(f"writing last parquet number into file: {last_parquet_file_num}")
                    write_to_file(
                        last_parquet_file_num,
                        collection_name,
                        LAST_PARQUET_FILE_NUMBER,
                        FileType.PICKLE,
                    )
                    # Only clear the batch (and the backup-recovery flag) once every write/push
                    # above has actually succeeded - otherwise a mid-flush failure would
                    # silently drop the batch instead of retrying it.
                    accumulative_df = None
                    if recovering_from_backup_token:
                        logger.info(
                            "resume_token checkpoint for collection %s successfully refreshed; "
                            "exiting backup-recovery replay window, resuming normal insert row markers.",
                            collection_name,
                        )
                        recovering_from_backup_token = False
                except Exception as exc:
                    logger.critical(
                        "DATA CONTINUITY RISK: failed to flush/push batch for collection %s: %s. "
                        "Batch retained in memory and will be retried on the next flush attempt.",
                        collection_name,
                        exc,
                        exc_info=True,
                    )
    return accumulative_df, last_sync_time, recovering_from_backup_token

def __bump_schema_version(
    collection_name,
    accumulative_df,
    resume_token,
    init_sync_stat_flag,
    last_sync_time,
    time_threshold_in_sec,
    offending_doc,
    logger,
    recovering_from_backup_token=False,
):
    # 1. Flush whatever is currently accumulated into the OLD versioned folder so
    # no buffered rows are lost across the boundary. The offending doc isn't in
    # accumulative_df yet (the signal returned before the concat step), so the
    # flush cleanly contains only docs that match the old schema.
    if accumulative_df is not None:
        accumulative_df, last_sync_time, recovering_from_backup_token = process_accumulative_df(
            accumulative_df,
            collection_name,
            init_sync_stat_flag,
            last_sync_time,
            time_threshold_in_sec,
            resume_token,
            logger,
            recovering_from_backup_token,
            force_flush=True,
        )

    # 2. Bump the schema version. From this point, get_table_dir/get_effective_table_name
    # return the new versioned directory and LZ folder.
    old_version = get_schema_version(collection_name)
    new_version = old_version + 1
    set_schema_version(collection_name, new_version)
    logger.info(
        "schema version for collection %s bumped from %d to %d",
        collection_name,
        old_version,
        new_version,
    )

    # 3. Reset in-memory schema state so process_dataframe re-seeds from scratch.
    schemas.reset_table_schema(collection_name)

    # 4. Carry forward state that must persist across versions. write_to_file
    # writes locally to the new versioned dir AND pushes to the new LZ folder.
    write_to_file(
        resume_token,
        collection_name,
        DELTA_SYNC_RESUME_TOKEN_FILE_NAME,
        FileType.PICKLE,
        backup_file_name=DELTA_SYNC_RESUME_TOKEN_BACKUP_FILE_NAME,
    )
    if recovering_from_backup_token:
        logger.info(
            "resume_token checkpoint for collection %s refreshed during schema-version bump; "
            "exiting backup-recovery replay window.",
            collection_name,
        )
        recovering_from_backup_token = False
    write_to_file(
        init_sync_stat_flag,
        collection_name,
        INIT_SYNC_STATUS_FILE_NAME,
        FileType.PICKLE,
    )

    # 5. Push fresh metadata files to the new LZ folder so Fabric can ingest
    # the new versioned table.
    push_table_metadata_files(collection_name)

    # 6. Re-seed schema by processing the offending doc as the first row of the
    # new version. With in-memory schema cleared, every column hits the
    # "new column" branch in process_dataframe and gets a fresh entry.
    new_df = pd.DataFrame([offending_doc])
    schema_utils.process_dataframe(collection_name, new_df)

    # 7. The first row in the new versioned table is, by definition, an insert
    # (the prior table is closed). Use the insert row marker regardless of the
    # underlying Mongo operationType.
    new_df.insert(0, ROW_MARKER_COLUMN_NAME, [CHANGE_STREAM_OPERATION_MAP["insert"]])

    return new_df, time.time(), recovering_from_backup_token


def __post_init_flush(table_name: str, logger):
    if not logger:
        logger = logging.getLogger(f"{__name__}[{table_name}]")
    logger.info(f"begin post init flush of delta change for collection {table_name}")
    current_dir = os.path.dirname(os.path.abspath(__file__))
    table_dir = get_table_dir(table_name)
    if not os.path.exists(table_dir):
        return
    temp_parquet_filename_list = sorted(
        [
            filename
            for filename in os.listdir(table_dir)
            if os.path.splitext(filename)[1] == ".parquet"
            and os.path.splitext(filename)[0].startswith(TEMP_PREFIX_DURING_INIT)
        ]
    )
    for temp_parquet_filename in temp_parquet_filename_list:
        temp_parquet_full_path = os.path.join(table_dir, temp_parquet_filename)
        try:
            # changed to get last parquet file number from LZ for resilience
            #new_parquet_full_path = get_parquet_full_path_filename(table_name)
            last_parquet_file_num = read_from_file(
                table_name, LAST_PARQUET_FILE_NUMBER, FileType.PICKLE
            )
            if not last_parquet_file_num:
                last_parquet_file_num = get_last_parquet_file_num_from_lz(table_name)
            new_parquet_full_path = get_parquet_full_path_filename(table_name, last_parquet_file_num)
            logger.debug("renaming temp parquet file")
            logger.debug(f"old name: {temp_parquet_full_path}")
            logger.debug(f"new name: {new_parquet_full_path}")
            logger.info(
                f"renaming parquet file from {temp_parquet_full_path} to {new_parquet_full_path}"
            )
            os.rename(temp_parquet_full_path, new_parquet_full_path)
            push_file_to_lz(new_parquet_full_path, table_name)
            # write last parquet file number to file
            last_parquet_file_num +=  1
            logger.info(f"writing last parquet number into file: {last_parquet_file_num}")
            write_to_file(
                last_parquet_file_num,
                table_name,
                LAST_PARQUET_FILE_NUMBER,
                FileType.PICKLE,
            )
        except Exception as exc:
            # Don't let one bad temp file abort the rest of the post-init flush (or
            # take the whole listening thread down with it).
            logger.critical(
                "DATA CONTINUITY RISK: failed to flush temp parquet file %s for collection %s: "
                "%s. Skipping to the next temp file; this file will need manual recovery.",
                temp_parquet_full_path,
                table_name,
                exc,
                exc_info=True,
            )
            continue
