import azure.functions as func
import azure.durable_functions as df
from flair.models import SequenceTagger

import logging

from src import despatch_job, shared_constants, shared_azure_dl


# function 1 call OCR on all files
dfApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# durable function 2 load model and call NER on all files
@dfApp.route(route="orchestrators/{functionName}")
@dfApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    logging.info("HTTP START")
    function_name = req.route_params.get("functionName")
    payload = despatch_job.parse_http_request(req)
    instance_id = await client.start_new(function_name, None, payload)
    response = client.create_check_status_response(req, instance_id)
    return response


@dfApp.orchestration_trigger(context_name="context")
def ner_orchestrator(context):
    logging.info("ORC TRIG")
    input_context = context.get_input()
    blob_path = input_context.get("blob_path")
    logging.info(f"Blob path {blob_path}")

    # load tagger
    # tagger = SequenceTagger.load(shared_constants.HF_NER_MODEL)

    # get list of file_ids
    file_list = shared_azure_dl.list_filenames_from_data_lake(blob_path)
    logging.info(f"{len(file_list)} files on blob {blob_path}")

    # call ner function for all file_ids

    # result1 = yield context.call_activity("ner", "Seattle")

    result1 = "Result 1"
    result2 = "Result 2"
    result3 = "Result 3"

    return [result1, result2, result3]


# @dfApp.activity_trigger(input_name="file_id")
# def ner(file_id: str):
#     logging.info(f"NER {file_id}")
#     return f"Hello {file_id}"


# app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# @app.route(route="ocr", auth_level=func.AuthLevel.ANONYMOUS)
# def ocr(req: func.HttpRequest) -> func.HttpResponse:

#     logging.info("OCR function invoked")
#     try:
#         response = despatch_job.despatch_job(req)
#         return response

#     except Exception as e:
#         error_msg = f"Failed: OCR operation error {e}"
#         logging.error(error_msg)

#         return func.HttpResponse(error_msg)
