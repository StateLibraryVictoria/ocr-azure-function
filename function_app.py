import azure.functions as func
import logging
from time import sleep

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
    path="raw/image-pipeline/image-capture/{folder}/{file_name}.jpg",
    connection="DATA_LAKE_CONNECTION_STRING",
)
def ocr_blob_trigger(blob: func.InputStream):
    logging.info(f"{blob.name} added to image pipeline")
    job_complete = despatch_job.despatch_blob_job(blob, "ocr")

    if job_complete == False:
        sleep(2)
        despatch_job.despatch_blob_job(blob, "ocr")


@app.blob_trigger(
    arg_name="blob",
    path="raw/image-pipeline/ocr/{folder}/{file_name}.txt",
    connection="DATA_LAKE_CONNECTION_STRING",
)
def ner_blob_trigger(blob: func.InputStream):
    logging.info(f"{blob.name} added to image pipeline")
    job_complete = despatch_job.despatch_blob_job(blob, "ner")
    if job_complete == False:
        sleep(2)
        despatch_job.despatch_blob_job(blob, "ner")


@app.blob_trigger(
    arg_name="blob",
    path="raw/image-pipeline/ner/{folder}/{file_name}.txt",
    connection="DATA_LAKE_CONNECTION_STRING",
)
def caption_blob_trigger(blob: func.InputStream):
    logging.info(f"{blob.name} added to image pipeline")
    job_complete = despatch_job.despatch_blob_job(blob, "caption")
    if job_complete == False:
        sleep(2)
        despatch_job.despatch_blob_job(blob, "caption")


@app.blob_trigger(
    arg_name="blob",
    path="raw/image-pipeline/caption/{folder}/{file_name}.txt",
    connection="DATA_LAKE_CONNECTION_STRING",
)
def ingest_file_trigger(blob: func.InputStream):
    logging.info(f"{blob.name} added to image pipeline")
    job_complete = despatch_job.despatch_blob_job(blob, "ingest-file")
    if job_complete == False:
        sleep(2)
        despatch_job.despatch_blob_job(blob, "ingest-file")


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
