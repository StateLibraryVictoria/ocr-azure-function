import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="ocr", auth_level=func.AuthLevel.ANONYMOUS)
def ocr(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("OCR function invoked")
    try:
        from .src import despatch_job

        response = despatch_job.despatch_job(req)
        return response

    except Exception as e:
        logging.error(f"An error occurred invoking OCR operation: {e}")


@app.route(route="named_entity_recognition", auth_level=func.AuthLevel.ANONYMOUS)
def named_entity_recognition(req: func.HttpRequest) -> func.HttpResponse:

    try:
        from src import despatch_job

        response = despatch_job.despatch_job(req)
        return response

    except Exception as e:
        logging.error(f"An error occurred invoking OCR operation: {e}")


@app.route(route="hello", auth_level=func.AuthLevel.ANONYMOUS)
def hello(req: func.HttpRequest) -> func.HttpResponse:

    try:
        import despatch_job

        response = despatch_job.despatch_job(req)
        return response

    except Exception as e:
        logging.error(f"An error occurred invoking OCR operation: {e}")
