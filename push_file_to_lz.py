import os
from dotenv import load_dotenv
import requests
import json
import logging
from datetime import datetime
from urllib.parse import urlparse, quote
import utils
import constants
from constants import METADATA_FILE_NAME, PARTNER_EVENTS_FILE_NAME

logger = logging.getLogger(__name__)


def push_file_to_lz(
    filepath: str,
    table_name: str,
):
    logger.info(f"pushing file to lz. table_name={table_name}, filepath={filepath}")
    try:
        if os.getenv("DEBUG__SKIP_PUSH_TO_LZ"):
            logger.info("Push to LZ skipped by environment variable DEBUG__SKIP_PUSH_TO_LZ")
        else:
            access_token = __get_access_token(
                os.getenv("APP_ID"), os.getenv("SECRET"), os.getenv("TENANT_ID")
            )
            __patch_file(access_token, filepath, os.getenv("LZ_URL"), table_name)
        # identify if any other parquet files in the dir, and remove them, leaving only the last one we just pushed
        __clean_up_old_parquet_files(filepath)
    except Exception as e:
        logger.error(f"Error pushing file to lz: {str(e)}")
        raise


def __clean_up_old_parquet_files(filepath: str):
    if os.getenv("DEBUG__SKIP_PARQUET_FILES_CLEAN_UP"):
        return
    filename_stem = os.path.splitext(os.path.basename(filepath))[0]
    filename_ext = os.path.splitext(os.path.basename(filepath))[1]
    # do nothing if it's a parquet file with prefix, or it's not a parquet file
    if not filename_stem.isnumeric() or filename_ext != ".parquet":
        return
    logger.debug(f"Cleaning up old parquet files. Current file: {filepath}")
    dir = os.path.dirname(filepath)
    current_filename = os.path.basename(filepath)
    old_parquet_filename_list = [
        filename
        for filename in os.listdir(dir)
        if os.path.splitext(filename)[1] == ".parquet"
        and os.path.splitext(filename)[0].isnumeric()
        and int(os.path.splitext(filename)[0]) < int(filename_stem)
    ]
    for old_parquet_filename in old_parquet_filename_list:
        logger.debug(f"Deleting old parquet file {old_parquet_filename}")
        os.remove(os.path.join(dir, old_parquet_filename))


def __get_access_token(app_id, client_secret, directory_id):
    """It will create a access token to access the mail apis"""
    app_id = app_id  # Application Id - on the azure app overview page
    client_secret = client_secret
    directory_id = directory_id
    token_url = (
        "https://login.microsoftonline.com/" + directory_id + "/oauth2/v2.0/token"
    )
    token_data = {
        "grant_type": "client_credentials",
        "client_id": app_id,
        "client_secret": client_secret,
        "scope": "https://storage.azure.com/.default",
    }
    token_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    # logger.debug(token_url)
    token_response = requests.post(token_url, data=token_data, headers=token_headers)
    token_response_dict = json.loads(token_response.text)

    # logger.debug(token_response.text)

    token = token_response_dict.get("access_token")

    if token == None:
        logger.debug("Unable to get access token")
        logger.debug(str(token_response_dict))
        raise Exception("Error in getting in access token")
    else:
        #   logger.debug("Token is:" + token)
        return token


def __patch_file(access_token, file_path, lz_url, table_name):
    try:
        file_name = os.path.basename(file_path)
        file_name_temp = file_name
        base_url = lz_url + utils.get_effective_table_name(table_name) + "/"
        if not file_name.startswith('_'):
            file_name_temp = '_' + file_name
        else:
            file_name_temp = file_name
            
        token_url_temp = base_url + file_name_temp + '_TEMP' + "?resource=file"
        token_url = base_url + file_name

        token_headers = {"Authorization": "Bearer " + access_token, "content-length": "0"}
        logger.debug("creating file in lake")

        # Code to create file in lakehouse
        response = requests.put(token_url_temp, data={}, headers=token_headers)
        logger.debug(response)
        # Do not proceed to append/rename if the temp file couldn't even be created,
        # otherwise a stale/empty file could later get renamed over a good checkpoint.
        response.raise_for_status()

        token_url_temp = base_url + file_name_temp + '_TEMP' + "?position=0&action=append&flush=true"
        token_headers = {
            "Authorization": "Bearer " + access_token,
            "x-ms-file-name": file_name,
        }
            
        logger.debug(token_url_temp)    
        logger.debug("pushing data to file in lake")

        # Code to push Data to Lakehouse
        with open(file_path, "rb") as file:
            file_contents = file.read()
            response = requests.patch(token_url_temp, data=file_contents, headers=token_headers)
        logger.debug(response)
        # A failed or partial append must never be promoted to the live file via rename,
        # otherwise the rename would replace a good checkpoint with an empty/corrupt one.
        response.raise_for_status()

        # Rename file from temp to actual name
        token_headers = {
            "Authorization": "Bearer " + access_token,
            "x-ms-rename-source": base_url + file_name_temp + '_TEMP' + "?resource=file",
            "x-ms-version": "2020-06-12"
        }
        response = requests.put(token_url, headers=token_headers)
        logger.debug(response)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error patching file to landing zone: {str(e)}")
        raise


def get_file_from_lz(table_name, file_name):
    logger.info(
        f"trying to get file from lz. table_name={utils.get_effective_table_name(table_name)}, file_name={file_name}"
    )
    access_token = __get_access_token(
        os.getenv("APP_ID"), os.getenv("SECRET"), os.getenv("TENANT_ID")
    )
    token_headers = {"Authorization": "Bearer " + access_token, "content-length": "0"}
    url = os.getenv("LZ_URL") + utils.get_effective_table_name(table_name) + "/" + file_name
    response = requests.get(url, headers=token_headers)
    response_status_code = response.status_code
    if response_status_code != 200:
        logger.warning(
            f"failed to get file from Landing Zone. Server responded with code {response_status_code}"
        )
        return None, None
    local_file_path = os.path.join(utils.get_table_dir(table_name), file_name)
    # Commented out to stop re-write
    # with open(local_file_path, "wb") as local_file:
    #     for chunk in response.iter_content():
    #         local_file.write(chunk)
    return (response_status_code, response)


def list_files_in_lz(table_name):
    """List the base file names directly under a collection's Landing Zone folder.

    Best-effort by design: returns an empty list on any error or non-200
    response. Callers use this only as a fallback source of truth, and must
    degrade gracefully (start numbering from scratch) rather than crash when the
    LZ can't be listed.
    """
    try:
        lz_url = os.getenv("LZ_URL")
        effective_name = utils.get_effective_table_name(table_name)
        # LZ_URL points at the OneLake *blob* endpoint, but the ADLS Gen2
        # "List Path" operation (which returns clean JSON) is served from the
        # *dfs* endpoint. Swap the host to reach it.
        dfs_url = lz_url.replace(".blob.", ".dfs.")
        parsed = urlparse(dfs_url)
        # The first path segment is the filesystem (the Fabric workspace); the
        # remainder is the directory prefix leading to the LandingZone.
        path = parsed.path.lstrip("/")
        segments = path.split("/", 1)
        filesystem = segments[0]
        directory_base = segments[1] if len(segments) > 1 else ""
        directory = (directory_base + effective_name).rstrip("/")

        access_token = __get_access_token(
            os.getenv("APP_ID"), os.getenv("SECRET"), os.getenv("TENANT_ID")
        )
        list_url = (
            f"{parsed.scheme}://{parsed.netloc}/{filesystem}"
            f"?resource=filesystem&recursive=false&directory={quote(directory)}"
        )
        token_headers = {"Authorization": "Bearer " + access_token}
        response = requests.get(list_url, headers=token_headers)
        if response.status_code != 200:
            logger.warning(
                "failed to list Landing Zone folder for %s. Server responded with code %s",
                effective_name,
                response.status_code,
            )
            return []
        paths = response.json().get("paths", [])
        file_names = [
            os.path.basename(entry.get("name", ""))
            for entry in paths
            # ADLS marks directories with isDirectory="true"; files omit it.
            if str(entry.get("isDirectory", "false")).lower() != "true"
        ]
        return [name for name in file_names if name]
    except Exception as e:
        logger.warning(
            f"error listing Landing Zone folder for table={table_name}: {e}"
        )
        return []


def get_last_parquet_file_num_from_lz(table_name) -> int:
    """Fallback used when the persisted parquet-file counter
    (LAST_PARQUET_FILE_NUMBER) is missing or corrupted.

    Derives the last-used parquet number from what is ACTUALLY present in the
    Landing Zone, rather than from local disk. The local data_files/ directory
    can drift out of sync with the LZ (e.g. a Fabric mirror is torn down and
    recreated - giving a fresh, empty LZ - while stale numbered parquet files
    remain locally). Deriving from local files in that case would resume
    numbering mid-sequence (e.g. ...0104.parquet) into a fresh LZ that Fabric
    expects to start at ...0001.parquet, so Fabric ingests nothing.

    Returns 0 (i.e. "nothing written yet", so numbering starts at ...0001) when
    the LZ folder is empty or can't be listed.
    """
    numbered = []
    for file_name in list_files_in_lz(table_name):
        stem, ext = os.path.splitext(file_name)
        if ext == ".parquet" and stem.isnumeric():
            numbered.append(int(stem))
    return max(numbered) if numbered else 0


def push_table_metadata_files(collection_name):
    current_dir = os.path.dirname(os.path.abspath(__file__))

    metadata_path = os.path.join(current_dir, METADATA_FILE_NAME)
    logger.info("writing metadata file to LZ")
    push_file_to_lz(metadata_path, collection_name)

    partner_events_template_path = os.path.join(current_dir, "_partnerEvents_template.json")
    partner_events_output_path = os.path.join(current_dir, PARTNER_EVENTS_FILE_NAME)
    logger.info("writing _partnerEvents.json file to LZ")
    with open(partner_events_template_path, 'r') as f:
        partner_events_content = f.read()
    partner_events_content = partner_events_content \
        .replace('${MONGO_DB_NAME}', os.getenv('MONGO_DB_NAME', '')) \
        .replace('${MONGO_COLLECTION}', utils.get_effective_table_name(collection_name)) \
        .replace('${LZ_URL}', os.getenv('LZ_URL', ''))
    with open(partner_events_output_path, 'w') as output_file:
        output_file.write(partner_events_content)
    push_file_to_lz(partner_events_output_path, collection_name)


def delete_file_from_lz(table_name, file_name):
    logger.info(
        f"trying to delete file from lz. table_name={table_name}, file_name={file_name}"
    )
    access_token = __get_access_token(
        os.getenv("APP_ID"), os.getenv("SECRET"), os.getenv("TENANT_ID")
    )
    token_headers = {"Authorization": "Bearer " + access_token, "content-length": "0"}
    url = os.getenv("LZ_URL") + utils.get_effective_table_name(table_name) + "/" + file_name
    response = requests.delete(url, headers=token_headers)
    logger.debug(f"delete response: {response}")
    return response.status_code if response.status_code == 200 else None
