import azure.functions as func

import logging

from src import despatch_job

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="ocr", auth_level=func.AuthLevel.ANONYMOUS)
def ocr(req: func.HttpRequest) -> func.HttpResponse:
    return despatch_job(req)


@app.route(route="ner", auth_level=func.AuthLevel.ANONYMOUS)
def ner(req: func.HttpRequest) -> func.HttpResponse:

    return despatch_job(req)


@app.route(route="caption", auth_level=func.AuthLevel.ANONYMOUS)
def caption(req: func.HttpRequest) -> func.HttpResponse:

    return despatch_job(req)


@app.route(route="ingest-file", auth_level=func.AuthLevel.ANONYMOUS)
def caption(req: func.HttpRequest) -> func.HttpResponse:

    return despatch_job(req)


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
