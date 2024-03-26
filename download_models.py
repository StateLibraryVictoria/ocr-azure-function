import logging

from flair.models import SequenceTagger

logging.info("DOWNLOAD models")
print("DOWNLOAD models")

try:

    SequenceTagger.load("flair/pos-english")
    logging.info("Model downloaded")
    print("Model downloaded")

except Exception as e:
    logging.error(f"Exception : {e}")
    print(f"Exception : {e}")
