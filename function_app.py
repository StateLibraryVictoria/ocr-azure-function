import azure.functions as func
import logging

from src import despatch_job

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="ocr", auth_level=func.AuthLevel.ANONYMOUS)
def ocr(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("OCR function invoked")
    try:

        response = despatch_job.despatch_job(req)
        return response

    except Exception as e:
        logging.error(f"An error occurred invoking OCR operation: {e}")

        return func.HttpResponse("OCR failed")


@app.route(route="named_entity_recognition", auth_level=func.AuthLevel.ANONYMOUS)
def named_entity_recognition(req: func.HttpRequest) -> func.HttpResponse:
    try:

        response = despatch_job.despatch_job(req)
        return response

    except Exception as e:
        logging.error(f"An error occurred invoking OCR operation: {e}")

        return func.HttpResponse("Named entity recognition failed")
