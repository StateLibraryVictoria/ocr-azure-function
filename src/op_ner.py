import logging
import pandas as pd

from . import shared_helpers, shared_azure_dl, shared_constants


def ner_ocr_output(file_id: str):

    ocr_df = shared_azure_dl.read_df_from_data_lake(
        "image-pipeline", "ocr", file_id, add_column_names=True
    )

    ocr_text = shared_helpers.convert_df_column_to_string(ocr_df, "text")

    payload = {"inputs": ocr_text}

    ner_list = shared_helpers.call_hf_model(
        shared_constants.HF_NER_MODEL, payload=payload
    )
    logging.info(f"{len(ner_list)} named entities recognised for {file_id}")
    if len(ner_df) == 1:
        logging.info(f"DF {ner_df}")

    ner_df = pd.DataFrame.from_dict(ner_list)

    upload = shared_azure_dl.upload_dataframe_to_data_lake("ner", ner_df, file_id)

    return upload
