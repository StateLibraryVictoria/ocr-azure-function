import pandas as pd

from . import shared_helpers, shared_azure_dl, shared_constants


def caption_image(file_id: str):

    image_data = shared_azure_dl.read_image_from_data_lake(file_id)

    image_caption = shared_helpers.call_hf_model(
        shared_constants.HF_IMAGE_CAPTION_MODEL, data=image_data
    )

    image_caption_df = pd.DataFrame.from_dict(image_caption)

    upload = shared_azure_dl.upload_dataframe_to_data_lake(
        "caption", image_caption_df, file_id
    )

    return upload
