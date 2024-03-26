from flair.models import SequenceTagger
from src import shared_constants

SequenceTagger.load(shared_constants.HF_NER_MODEL)
