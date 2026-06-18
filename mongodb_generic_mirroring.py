import os
import logging
from threading import Thread
import pymongo
from pymongo.errors import ServerSelectionTimeoutError
from dotenv import load_dotenv
import json

from init_sync import init_sync
from listening import listening
from schema_utils import init_table_schema
from constants import (
    METADATA_FILE_NAME,
    PARTNER_EVENTS_FILE_NAME
)
from push_file_to_lz import push_table_metadata_files
from file_utils import FileType, read_from_file
from logging_config import setup_logging

logger = logging.getLogger(__name__)

def mirror():
    load_dotenv()
    setup_logging()
    if (
        not os.getenv("MONGO_CONN_STR")
        or not os.getenv("MONGO_DB_NAME")
        or not os.getenv("MONGO_COLLECTION")
        or not os.getenv("LZ_URL")
        or not os.getenv("APP_ID")
        or not os.getenv("SECRET")
        or not os.getenv("TENANT_ID")
        or not os.getenv("INIT_LOAD_BATCH_SIZE")
        or not os.getenv("DELTA_SYNC_BATCH_SIZE")
    # added threshold time    
        or not os.getenv("TIME_THRESHOLD_IN_SEC")
    ):
        raise ValueError("Missing environment variable.")

    mongodb_coll_name = os.getenv("MONGO_COLLECTION")
    collection_list = []
    all_collections = __get_all_collections()
    if mongodb_coll_name == "all":
        collection_list = all_collections
    elif mongodb_coll_name.startswith("["):
        collection_list = json.loads(mongodb_coll_name)
    elif isinstance(mongodb_coll_name, str):
        collection_list = [mongodb_coll_name]
    else:
        raise ValueError(
            'Invalid parameter value: mongodb_coll_name. "\
            "Expected a list of collection names, a str of a single collection"\
            " name, or "all" for all collections in the database.'
        )

    # threads: list[Thread] = []

    # remove non-exists collections
    removed_collections = []
    collection_list = [
        item
        for item in collection_list
        if item in all_collections or removed_collections.append(item) is None
    ]
    for non_exists_collection in removed_collections:
        logger.warning(f"removed non-exists collection {non_exists_collection}")

    for collection_name in collection_list:
        metadata_file_exists = read_from_file(
            collection_name, METADATA_FILE_NAME, FileType.TEXT
        )
        partner_events_file_exists = read_from_file(
            collection_name, PARTNER_EVENTS_FILE_NAME, FileType.TEXT
        )
        if not metadata_file_exists or not partner_events_file_exists:
            push_table_metadata_files(collection_name)

        init_table_schema(collection_name)

        Thread(target=listening, args=(collection_name,)).start()
        # listener_thread = Thread(target=listening, args=(collection_name,))
        # listener_thread.start()
        # threads.append(listener_thread)

        # Moved the starting of init_sync to listening so as not to miss any records which may come by the time we start init_sync
        # Thread(target=init_sync, args=(collection_name,)).start()
        # init_thread = Thread(target=init_sync, args=(collection_name,))
        # init_thread.start()
        # threads.append(init_thread)

    # for thread in threads:
    #     thread.join()

    # mirror() returns after starting the (non-daemon) listener threads, which keep
    # the process alive on their own. This lets mirror() be used both from the Flask
    # entry point (app.py) and from a headless context such as a Windows service
    # (service_runner.py), where there is no console stdin to read a "quit" command.
    logger.info("Mirroring started for collections: %s", collection_list)


def __get_all_collections() -> list[str]:
    client = pymongo.MongoClient(os.getenv("MONGO_CONN_STR"))
    # check database existence
    db_name = os.getenv("MONGO_DB_NAME")
    logger.debug("db_name=%s", db_name)
    try:
        all_db_names = client.list_database_names()
        if db_name not in all_db_names:
            raise ValueError(f"Database name provided do not exists: {db_name}")
        db = client[db_name]
        return db.list_collection_names()
    except ServerSelectionTimeoutError:
        raise ValueError("Can not connect to MongoDB with the provided MONGO_CONN_STR.")


if __name__ == "__main__":
    import sys

    mirror()
    # When run interactively, keep supporting the manual "quit" command. In a
    # non-interactive context (e.g. a service) there is no stdin, so just block
    # on the listener threads instead of crashing on EOF.
    if sys.stdin and sys.stdin.isatty():
        while True:
            cmd = input()
            if cmd.lower() == "quit":
                os._exit(0)
    else:
        import threading

        for thread in threading.enumerate():
            if thread is not threading.current_thread():
                thread.join()
