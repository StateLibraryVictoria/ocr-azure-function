import logging
import pytesseract
import io
from PIL import Image

from . import shared_azure_dl


def ocr_image(file_id: str, lang="eng"):

    logging.info(f"Beginning OCR for {file_id}")

    image_data = shared_azure_dl.read_image_from_data_lake(file_id)

    image = Image.open(io.BytesIO(image_data))

    result_df = pytesseract.image_to_data(image, lang=lang, output_type="data.frame")

    uploaded = shared_azure_dl.upload_dataframe_to_data_lake("ocr", result_df, file_id)

    return uploaded
