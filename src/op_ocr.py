import logging
import pytesseract

# from .shared_azure_dl import read_image_from_data_lake, upload_dataframe_to_data_lake
from . import shared_azure_dl


def ocr_image(file_path: str, file_id: str, lang="eng"):

    logging.info(f"Beginning OCR for {file_id}")

    image = shared_azure_dl.read_image_from_data_lake(file_path)

    result_df = pytesseract.image_to_data(image, lang=lang, output_type="data.frame")

    uploaded = shared_azure_dl.upload_dataframe_to_data_lake("ocr", result_df, file_id)

    return uploaded
