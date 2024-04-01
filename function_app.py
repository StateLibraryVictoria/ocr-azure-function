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
        error_msg = f"Failed: OCR operation error {e}"
        logging.error(error_msg)

        return func.HttpResponse(error_msg)


@app.route(route="ner", auth_level=func.AuthLevel.ANONYMOUS)
def ner(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("Named entity recognition function invoked")
    try:
        response = despatch_job.despatch_job(req)
        return response

    except Exception as e:
        error_msg = f"Failed: ner operation error {e}"
        logging.error(error_msg)

        return func.HttpResponse(error_msg)


@app.route(route="caption", auth_level=func.AuthLevel.ANONYMOUS)
def caption(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("Image caption function invoked")
    try:
        response = despatch_job.despatch_job(req)
        return response

    except Exception as e:
        error_msg = f"Failed: image caption operation error {e}"
        logging.error(error_msg)

        return func.HttpResponse(error_msg)



@app.blob_trigger(arg_name="myblob", path="image-pipeline/image-capture",
                               connection="slvdevdatalake_STORAGE") 
def BlobTrigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
