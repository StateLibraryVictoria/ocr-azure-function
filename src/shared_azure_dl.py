import logging
import os
import io
from PIL import Image

from azure.storage.filedatalake import DataLakeFileClient, DataLakeServiceClient


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


# upload to dl
# sample_filepath = "image-pipeline/image-capture/018/WIN_20240318_09_50_20_Pro.jpg"

# dl_image = read_image_from_data_lake(sample_filepath)
# dl_image.show()
