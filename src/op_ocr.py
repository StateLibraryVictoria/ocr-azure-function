import logging
import pytesseract

from .shared_azure_dl import read_image_from_data_lake


def ocr_image(file_path: str, file_id: str, lang="eng"):

    logging.info(f"Beginning OCR for {file_id}")

    image = read_image_from_data_lake(file_path)

    result = pytesseract.image_to_data(image, lang=lang)

    return result
