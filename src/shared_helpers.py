import os
import pandas as pd
import requests

# from . import shared_constants
import shared_constants


def get_file_id(file_path: str):
    """Converts the file-path of the Azure storage blob to a consistent id, allowing an image and it's derivatives to be tracked throughout the pipeline

    Args:
        file_path (str): blob storage file path

    Returns:
        file_id (str): the identifier for an image and its derivatives
        bool: False value returned when a file path cannot be used to generate a file_id
    """

    file_path_stem = file_path.split(".")
    if len(file_path_stem) > 1:
        file_path_stem = ".".join(file_path_stem[:-1])
    else:
        file_path_stem = ".".join(file_path_stem)

    file_id = file_path_stem.split("/")
    if len(file_id) <= 2:
        return False
    file_id = "/".join(file_id[2:])

    return file_id


def convert_df_column_to_string(
    df: pd.DataFrame, column_name: str, remove_nan=True
) -> str:

    column_to_convert = df[column_name]

    if remove_nan:
        column_to_convert = column_to_convert[column_to_convert != "nan"]

    column_str = column_to_convert.str.cat(sep=" ")

    return column_str


def get_named_entities(payload: dict) -> list:

    headers = {"Authorization": f"Bearer {os.environ.get('HF_API_KEY')}"}

    response = requests.post(
        shared_constants.HF_API_URL_STEM, headers=headers, json=payload
    )

    if response.status_code == 503:
        payload["wait_for_model"] = True
        return get_named_entities(payload)

    return response.json()


def call_hf_model(model: str, payload={}, data={}):

    hf_model_api = f"{shared_constants.HF_API_URL_STEM}/{model}"

    headers = {"Authorization": f"Bearer {os.environ.get('HF_API_KEY')}"}

    response = requests.post(hf_model_api, headers=headers, json=payload, data=data)
    if response.status_code == 503:
        payload["wait_for_model"] = True
        return call_hf_model(model, payload=payload, data=data)

    return response.json()
