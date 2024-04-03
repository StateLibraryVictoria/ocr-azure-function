from dateutil.parser import parse
import logging
import pandas as pd

from . import shared_helpers, shared_azure_dl


def parse_date_entities(ner_df: pd.DataFrame) -> str:

    dates_df = ner_df.loc[ner_df["entity_group"] == "DATE"]

    try:

        dates_df["parsed_date"] = dates_df["word"].apply(parse)

    except:

        dates_df["parsed_date"] = dates_df["word"]

    parsed_dates = "\n".join(dates_df["parsed_date"].astype(str))

    return parsed_dates


def format_named_entities(ner_df: pd.DataFrame, min_confidence=0.50) -> str:

    ner_df["score"] = pd.to_numeric(ner_df["score"])
    ner_df = ner_df.loc[ner_df["score"] > min_confidence]
    ner_df = ner_df.sort_values(by="entity_group")

    ner_df["formatted_ner"] = ner_df["word"] + " (" + ner_df["entity_group"] + ")"
    named_entity_str = "\n".join(ner_df["formatted_ner"].to_list())

    return named_entity_str


def retrieve_image_caption(file_id: str) -> str:

    caption_df = shared_azure_dl.read_df_from_data_lake(
        "image-pipeline", "caption", file_id
    )

    image_caption = "".join(caption_df["generated_text"].to_list())

    return image_caption


def generate_ingest_row(file_id: str) -> dict:
    logging.info(f"Generating ingest row for {file_id}")
    try:

        ner_df = shared_azure_dl.read_df_from_data_lake(
            "image-pipeline", "ner", file_id
        )

        if type(ner_df) == pd.DataFrame:

            formatted_ner = format_named_entities(ner_df)
            parsed_date = parse_date_entities(ner_df)
        else:
            formatted_ner = ""
            parsed_date = ""

        image_caption = retrieve_image_caption(file_id)

        ocr_df = shared_azure_dl.read_df_from_data_lake(
            "image-pipeline", "ocr", file_id, add_column_names=True
        )

        if type(ocr_df) == pd.DataFrame:
            ocr_text = shared_helpers.convert_df_column_to_string(ocr_df, "text")
        else:
            ocr_text = ""

        return {
            "Title": file_id,
            "Date(1) Begin": parsed_date,
            "Extent number": "",
            "Top Container [indicator]": "",
            "barcode": "",
            "Image caption": image_caption,
            "OCR text": ocr_text,
            "Named entities": formatted_ner,
            "General": "\n".join([ocr_text, image_caption, formatted_ner]),
        }

    except Exception as e:

        logging.error(f"An error occurred generating an ingest row for {file_id}: {e}")
        print(f"An error occurred generating an ingest row for {file_id}: {e}")
        return {}


def generate_ingest_file(file_id: str) -> bool:

    ingest_row = generate_ingest_row(file_id)
    folder_name = file_id.split("/")[0]

    upload_df = pd.DataFrame(ingest_row)
    logging.info(f"Ingest file generated for generated for {file_id}")

    upload_df["Top Container [indicator]"] = folder_name
    upload_ingest_file = shared_azure_dl.upload_dataframe_to_data_lake(
        "ingest-file", upload_df, file_id
    )

    return upload_ingest_file


# def generate_ingest_file(filepath: str) -> bool:

#     folder_name = shared_helpers.get_folder_name(filepath)

#     logging.info(f"Generating ingest file for {folder_name}")

#     image_captures = shared_azure_dl.list_filenames_from_data_lake(filepath)
#     file_ids = [shared_helpers.get_file_id(capture) for capture in image_captures]

#     ingest_list = []
#     for file_id in file_ids:
#         ingest_row = generate_ingest_row(file_id)
#         ingest_list.append(ingest_row)

#     upload_df = pd.DataFrame(ingest_list)
#     logging.info(f"{len(upload_df)} rows generated for {folder_name}")

#     upload_df["Top Container [indicator]"] = folder_name
#     upload_ingest_file = shared_azure_dl.upload_dataframe_to_data_lake(
#         "ingest-file", upload_df, folder_name, file_suffix="csv"
#     )

#     return upload_ingest_file
