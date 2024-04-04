import azure.functions as func
import logging

from . import (
    op_ner,
    op_ocr,
    op_image_caption,
    op_generate_ingest_file,
    shared_helpers,
)


def get_operation_function(operation):

    operations = {
        "ocr": op_ocr.ocr_image,
        "ner": op_ner.ner_ocr_output,
        "caption": op_image_caption.caption_image,
        "ingest-file": op_generate_ingest_file.generate_ingest_file,
    }

    op_function = operations.get(operation)

    return op_function


def parse_http_request(req: func.HttpRequest) -> dict:

    request_params = req.params

    request_body = req.get_json()

    parsed_request = {**request_params, **request_body}

    return parsed_request


def despatch_job(req: func.HttpRequest) -> func.HttpResponse:

    request_params = parse_http_request(req)

    if not request_params:
        return False

    operation = request_params.get("operation")
    op_function = get_operation_function(operation)

    logging.info(f"{operation} function invoked")

    try:

        file_path = request_params.get("file_path")
        folder_path = request_params.get("folder_path")

        if file_path:
            file_id = shared_helpers.get_file_id(file_path)
            operation_completed = op_function(file_id)

        elif folder_path:
            operation_completed = op_function(folder_path)

        logging.info(f"{operation} complete")
        return func.HttpResponse(str(operation_completed))

    except Exception as e:
        error_msg = f"Failed: {operation} operation error {e}"
        logging.error(error_msg)

        return func.HttpResponse(error_msg)


def despatch_blob_job(blob: func.InputStream, operation: str) -> bool:

    file_id = shared_helpers.get_file_id(blob.name)

    try:

        logging.info(f"Invoking {operation} on {file_id}")

        op_function = get_operation_function(operation)

        invoke_operation = op_function(file_id)

        logging.info(f"{operation} invoked and complete: {invoke_operation}")

        return invoke_operation

    except Exception as e:

        logging.error(f"Error encountered while invoking {operation} for {file_id}")

        return False
