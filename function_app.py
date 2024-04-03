import azure.functions as func
import logging

from src import despatch_job

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="ocr", auth_level=func.AuthLevel.ANONYMOUS)
def ocr(req: func.HttpRequest) -> func.HttpResponse:
    result = despatch_job.despatch_job(req)
    return result


@app.route(route="ner", auth_level=func.AuthLevel.ANONYMOUS)
def ner(req: func.HttpRequest) -> func.HttpResponse:
    result = despatch_job.despatch_job(req)
    return result


@app.route(route="caption", auth_level=func.AuthLevel.ANONYMOUS)
def caption(req: func.HttpRequest) -> func.HttpResponse:
    result = despatch_job.despatch_job(req)
    return result


@app.route(route="ingest-file", auth_level=func.AuthLevel.ANONYMOUS)
def ingest_file(req: func.HttpRequest) -> func.HttpResponse:
    result = despatch_job.despatch_job(req)
    return result


@app.blob_trigger(
    arg_name="blob",
    path="raw/image-pipeline/{input_operation}/{folder}/{file_name}.jpg",
    connection="DATA_LAKE_CONNECTION_STRING",
)
def image_processing_blob_trigger(blob: func.InputStream):
    logging.info(f"{blob.name} added to image pipeline")
    despatch_job.despatch_blob_job(blob)


# @app.blob_trigger(
#     arg_name="myblob",
#     path="raw/image-pipeline/image-capture/{folder}/{file_name}.jpg",
#     connection="DATA_LAKE_CONNECTION_STRING",
# )
# def image_pipeline(myblob: func.InputStream):
#     logging.info(
#         f"Python blob trigger function processed blob"
#         f"Name: {myblob.name}"
#         f"Blob Size: {myblob.length} bytes"
#     )
