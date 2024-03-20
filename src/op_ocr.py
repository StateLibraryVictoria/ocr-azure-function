import logging
import pytesseract

from .shared_azure_dl import read_image_from_data_lake, upload_dataframe_to_data_lake


def ocr_image(file_path: str, file_id: str, lang="eng"):

    logging.info(f"Beginning OCR for {file_id}")

    image = read_image_from_data_lake(file_path)

    result_df = pytesseract.image_to_data(image, lang=lang, output_type="data.frame")

    uploaded = upload_dataframe_to_data_lake("ocr", result_df, file_id)

    return uploaded
