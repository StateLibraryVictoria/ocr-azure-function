import logging
import os
import io
from PIL import Image
import pandas as pd

from azure.storage.filedatalake import DataLakeFileClient


def read_file_from_data_lake(file_path: str, file_system="raw"):
    connection_str = os.environ.get("DATA_LAKE_CONNECTION_STRING")

    logging.info(f"Retrieving {file_path} from {file_system}")

    try:
        file_client = DataLakeFileClient.from_connection_string(
            connection_str, file_system, file_path
        )

        download = file_client.download_file()
        downloaded_file = download.readall()

        return downloaded_file

    except Exception as e:
        logging.error(
            f"The following error occurred when trying to read file: {file_path}. Here's the error: {e}"
        )
        return False


def read_image_from_data_lake(file_path: str, file_system="raw"):
    try:
        data_lake_image_bytes = read_file_from_data_lake(file_path)

        image = Image.open(io.BytesIO(data_lake_image_bytes))

        return image

    except Exception as e:
        logging.error(
            f"An error occurred whilst reading {file_path} from {file_system}, here is the error {e}"
        )
        return False


def upload_to_data_lake(file_system: str, file_path: str, file_contents: str) -> bool:
    connection_str = os.environ.get("DATA_LAKE_CONNECTION_STRING")
    logging.info(f"Adding blob: {file_path} to: {file_system}")

    try:
        file_client = DataLakeFileClient.from_connection_string(
            connection_str, file_system, file_path, connection_timeout=14400
        )

        file_client.create_file()
        file_client.append_data(file_contents, offset=0, length=len(file_contents))
        file_client.flush_data(len(file_contents))
        logging.info(f"{file_path} uploaded successfully to {file_system}")

        return True

    except Exception as e:
        logging.error(
            f"An error occurred when trying to create file: {file_path}. Here's the error: {e}"
        )
        return False


def upload_dataframe_to_data_lake(
    operation: str,
    dataframe: pd.DataFrame,
    file_id: str,
    delimiter=",",
    file_system="raw",
    pipeline="image-pipeline",
    file_suffix="txt",
):
    file_path = f"{pipeline}/{operation}/{file_id}.{file_suffix}"

    content_list = dataframe.values.tolist()

    # if columns_supplied:
    #     content_list.insert(0, list(dataframe.columns))

    file_contents = [f"{delimiter}".join(map(str, content)) for content in content_list]
    file_contents = "\n".join(file_contents)

    uploaded = upload_to_data_lake(file_system, file_path, file_contents)

    return uploaded
