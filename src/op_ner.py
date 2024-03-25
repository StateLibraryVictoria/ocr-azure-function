import logging
import pandas as pd
import re

from flair.data import Sentence
from segtok.segmenter import split_single


from . import shared_helpers, shared_azure_dl


def get_named_entities(ocr_text: str, tagger) -> list:

    sentence = [Sentence(sent, use_tokenizer=True) for sent in split_single(ocr_text)]

    tagger.predict(sentence)

    entities = []

    for token in sentence:
        for entity in token.get_spans("ner"):
            entity = str(entity)
            entities.append(entity)

    logging.info(f"{len(entities)} named entities recognised")

    return entities


get_named_entities("Melbourne University is a university 10th Feb 2024")


def format_named_entity(entity: str) -> dict:

    span_re = re.search(r"\d+:\d+", entity)

    named_entity_re = re.search(r'(?<=")(.*?)(?=")', entity)

    category_re = re.search(r"(?<=→\s)(.*?)(?=\s\()", entity)

    score_re = re.search(r"(?<=\()(.*?)(?=\))", entity)

    formatted_entity = {
        "span": span_re.group(),
        "named_entity": named_entity_re.group(),
        "category": category_re.group(),
        "score": score_re.group(),
    }

    return formatted_entity


def ner_ocr_output(file_id: str, tagger=None):

    ocr_df = shared_azure_dl.read_df_from_data_lake(
        "image-pipeline", "ocr", file_id, add_column_names=False
    )

    ocr_text = shared_helpers.convert_df_column_to_string(ocr_df, "text")

    ner = get_named_entities(ocr_text, tagger)

    ner_list = [format_named_entity(entity) for entity in ner]

    ner_df = pd.DataFrame(ner_list)

    upload = shared_azure_dl.upload_dataframe_to_data_lake("ner", ner_df, file_id)

    return upload
