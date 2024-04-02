import azure.functions as func

import logging

from src import despatch_job

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def handle_route(req: func.HttpRequest, route_name: str):
    logging.info(f"{route_name} function invoked")
    try:
        response = despatch_job.despatch_job(req)
        return response

    except Exception as e:
        error_msg = f"Failed: {route_name} operation error {e}"
        logging.error(error_msg)

        return func.HttpResponse(error_msg)


@app.route(route="ocr", auth_level=func.AuthLevel.ANONYMOUS)
def ocr(req: func.HttpRequest) -> func.HttpResponse:
    return handle_route(req, "OCR")


@app.route(route="ner", auth_level=func.AuthLevel.ANONYMOUS)
def ner(req: func.HttpRequest) -> func.HttpResponse:

    return handle_route(req, "Named entity recognition")


@app.route(route="caption", auth_level=func.AuthLevel.ANONYMOUS)
def caption(req: func.HttpRequest) -> func.HttpResponse:

    return handle_route(req, "Image caption")


@app.route(route="ingest-file", auth_level=func.AuthLevel.ANONYMOUS)
def caption(req: func.HttpRequest) -> func.HttpResponse:

    return handle_route(req, "Generate ingest file")


@app.blob_trigger(
    arg_name="raw",
    path="image-pipeline/image-capture",
    connection="DATA_LAKE_CONNECTION_STRING",
)
def BlobTrigger(myblob: func.InputStream):
    logging.info(
        f"Python blob trigger function processed blob"
        f"Name: {myblob.name}"
        f"Blob Size: {myblob.length} bytes"
    )
