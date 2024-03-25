import azure.functions as func
import logging
from flair.models import SequenceTagger

from src import despatch_job, shared_constants

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
ner_tagger = SequenceTagger.load(shared_constants.HF_NER_MODEL)


@app.route(route="ocr", auth_level=func.AuthLevel.ANONYMOUS)
def ocr(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("OCR function invoked")
    try:
        response = despatch_job.despatch_job(req)
        return response

    except Exception as e:
        error_msg = f"Failed: OCR operation error {e}"
        logging.error(error_msg)

        return func.HttpResponse(error_msg)


@app.route(route="ner", auth_level=func.AuthLevel.ANONYMOUS)
def ner(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("NER function invoked")

    try:
        logging.info(f"NER tagger {ner_tagger}")
        response = despatch_job.despatch_job(req, kwargs={"tagger": ner_tagger})
        return response

    except Exception as e:
        error_msg = f"Failed: NER operation error {e}"
        logging.error(error_msg)

        return func.HttpResponse(error_msg)
