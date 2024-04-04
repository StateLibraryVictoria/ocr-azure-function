import logging
import os
import pandas as pd
import re
from unidecode import unidecode

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


def read_image_from_data_lake(file_id: str, file_system="raw", image_file_suffix="jpg"):
    try:

        file_path = f"image-pipeline/image-capture/{file_id}.{image_file_suffix}"

        data_lake_image_bytes = read_file_from_data_lake(file_path)

        return data_lake_image_bytes

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

        if file_contents:
            file_client.append_data(file_contents, offset=0, length=len(file_contents))
            file_client.flush_data(len(file_contents))

        file_client.close()

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
    columns_supplied=True,
):
    file_path = f"{pipeline}/{operation}/{file_id}.{file_suffix}"

    if len(dataframe) > 0:

        dataframe = dataframe.replace(r"\n", " ", regex=True)
        dataframe = dataframe.replace(r"\r", " ", regex=True)
        dataframe = dataframe.replace(r"\x0c", " ", regex=True)

        # clean up special characters for all text columns
        object_columns = list(dataframe.select_dtypes(include=["object"]).columns)
        dataframe[object_columns] = dataframe[object_columns].astype("str")
        for col in object_columns:
            dataframe[col] = dataframe[col].apply(unidecode)
            dataframe[col] = dataframe[col].replace(
                re.escape(delimiter), " ", regex=True
            )

        content_list = dataframe.values.tolist()

        if columns_supplied:
            content_list.insert(0, list(dataframe.columns))

        file_contents = [
            f"{delimiter}".join(map(str, content)) for content in content_list
        ]
        file_contents = "\n".join(file_contents)

    else:
        logging.info(f"Dataframe supplied for {file_id} empty")
        file_contents = ""

    uploaded = upload_to_data_lake(file_system, file_path, file_contents)

    return uploaded


def read_df_from_data_lake(
    pipeline: str,
    operation: str,
    file_id: str,
    file_suffix="txt",
    delimiter=",",
    add_column_names=True,
    file_system="raw",
):

    file_path = f"{pipeline}/{operation}/{file_id}.{file_suffix}"

    file_contents = read_file_from_data_lake(file_path)
    file_contents = file_contents.decode("utf-8")

    if not file_contents:
        logging.error(f"Unable to create dataframe for {file_system}/{file_path}")
        return False
    split_contents = [
        label.split(f"{delimiter}") for label in file_contents.splitlines()
    ]

    logging.info(f"{len(split_contents)} rows read from {file_path}")

    df = pd.DataFrame.from_dict(split_contents)

    if add_column_names:
        df = df.rename(columns=df.iloc[0])
    df = df.drop(df.index[0])

    logging.info(f"{len(df)} rows in df {file_path}")

    return df


def list_filenames_from_data_lake(path: str, file_system="raw"):
    connection_str = os.environ.get("DATA_LAKE_CONNECTION_STRING")

    datalake_service_client = DataLakeServiceClient.from_connection_string(
        connection_str
    )
    file_system_client = datalake_service_client.get_file_system_client(file_system)

    path_list = file_system_client.get_paths(path)

    file_list = [path.get("name") for path in path_list]

    return file_list
