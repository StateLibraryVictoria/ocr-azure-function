import logging

from flair.models import SequenceTagger

logging.info("DOWNLOAD models")

try:

    SequenceTagger.load("flair/pos-english")
    logging.info("Model downloaded")

except Exception as e:
    logging.error(f"Exception : {e}")
