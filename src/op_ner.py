import pandas as pd
import os

from . import shared_helpers, shared_azure_dl, shared_constants

import requests


def get_named_entities(payload: dict) -> list:

    headers = {"Authorization": f"Bearer {os.environ.get('HF_API_KEY')}"}

    response = requests.post(
        shared_constants.HF_NER_API_URL, headers=headers, json=payload
    )

    if response.status_code == 503:
        payload["wait_for_model"] = True
        return get_named_entities(payload)

    return response.json()


def ner_ocr_output(file_id: str):

    ocr_df = shared_azure_dl.read_df_from_data_lake(
        "image-pipeline", "ocr", file_id, add_column_names=True
    )

    ocr_text = shared_helpers.convert_df_column_to_string(ocr_df, "text")

    payload = {"inputs": ocr_text}

    ner_list = get_named_entities(payload)

    ner_df = pd.DataFrame(ner_list)

    upload = shared_azure_dl.upload_dataframe_to_data_lake("ner", ner_df, file_id)

    return upload
