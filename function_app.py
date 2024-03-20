import azure.functions as func
import logging

from despatch_job import despatch_job

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="ocr", auth_level=func.AuthLevel.FUNCTION)
def ocr(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("OCR function invoked")
    try:
        response = despatch_job(req)
        return response

    except Exception as e:
        logging.error("An error occurred invoking OCR operation: {e}")
