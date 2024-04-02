from dateutil.parser import parse
import logging
import pandas as pd

# from . import shared_helpers, shared_azure_dl, shared_constants

import shared_helpers, shared_azure_dl, shared_constants


columns = [
    "Title",
    "Date(1) Begin",
    "Extent number",
    "Top Container [indicator]",
    "barcode",
    "General",
]


def parse_date_entities(ner_df: pd.DataFrame) -> str:

    dates_df = ner_df.loc[ner_df["entity_group"] == "DATE"]

    dates_df["parsed_date"] = dates_df["word"].apply(parse)

    parsed_dates = "\n".join(dates_df["parsed_date"])

    return parsed_dates


def format_named_entities(ner_df: pd.DataFrame, min_confidence=0.50) -> str:

    ner_df["score"] = pd.to_numeric(ner_df["score"])
    ner_df = ner_df.loc[ner_df["score"] > min_confidence]
    ner_df = ner_df.sort_values(by="entity_group")

    ner_df["formatted_ner"] = ner_df["word"] + " (" + ner_df["entity_group"] + ")"
    named_entity_str = "\n".join(ner_df["formatted_ner"].to_list())

    return named_entity_str


#  function: get container name


#  function: get image caption


def ingest_file(file_id: str) -> bool:

    ner_df = shared_azure_dl.read_df_from_data_lake("image-pipeline", "ner", file_id)

    formatted_ner = format_named_entities(ner_df)

    parsed_date = parse_date_entities(ner_df)


ingest_file("018-POC/WIN_20240318_09_32_44_Pro")
