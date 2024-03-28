import azure.functions as func
import logging

from . import op_ner, op_ocr, op_image_caption, shared_helpers


def get_operation_function(operation):

    operations = {
        "ocr": op_ocr.ocr_image,
        "ner": op_ner.ner_ocr_output,
        "caption": op_image_caption.caption_image,
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
    file_path = request_params.get("file_path")
    file_id = shared_helpers.get_file_id(file_path)

    op_function = get_operation_function(operation)

    operation_completed = op_function(file_id)

    if operation_completed:
        logging.info(f"{operation} complete for {file_id}")
        return func.HttpResponse(str(operation_completed))

    else:
        logging.error(f"{operation} could not be completed for {file_id}")
