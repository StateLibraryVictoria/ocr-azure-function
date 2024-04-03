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


def get_blob_function(input_operation):
    operations = {
        "image-capture": op_ocr.ocr_image,
        "ocr": op_ner.ner_ocr_output,
        "ner": op_image_caption.caption_image,
        "caption": op_generate_ingest_file.generate_ingest_file,
    }

    op_function = operations.get(input_operation)

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


def despatch_blob_job(blob: func.InputStream) -> bool:

    input_operation = blob.name.split("/")[2]
    output_operation = get_blob_function(input_operation)
    if not output_operation:
        logging.error(f"Could not match an output operation for {input_operation}")
        return False

    file_id = shared_helpers.get_file_id(blob.name)

    logging.info(f"Invoking {output_operation} on {file_id}")

    invoke_operation = output_operation(file_id)

    logging.info(f"{output_operation} invoked and complete: {invoke_operation}")

    return invoke_operation
