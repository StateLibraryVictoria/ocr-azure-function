import json
import pytest
import azure.functions as func

from despatch_job import get_operation_function, parse_http_request, despatch_job
from src.op_ocr import ocr_image


@pytest.mark.parametrize(
    "test_input, expected",
    [
        ({"operation": "ocr"}, ocr_image),
        ({"operation": "Unfound"}, None),
    ],
)
def test_get_operation_function(test_input, expected):
    assert get_operation_function(**test_input) == expected


def generate_test_request(test_params: dict) -> func.HttpRequest:

    test_req = func.HttpRequest(
        method="POST",
        body=json.dumps({}).encode("utf8"),
        url="/api/HttpTrigger",
        params=test_params,
    )

    return test_req


ocr_req_params = {
    "file_path": "image-pipeline/image-capture/101/file.jpg",
    "operation": "ocr",
}


@pytest.mark.parametrize(
    "test_input, expected",
    [({"req": generate_test_request(ocr_req_params)}, ocr_req_params)],
)
def test_parse_http_request(test_input, expected):
    assert parse_http_request(**test_input) == expected


@pytest.mark.parametrize(
    "test_input, expected",
    [({"req": generate_test_request(ocr_req_params)}, func.HttpResponse)],
)
def test_despatch_job(test_input, expected):

    assert isinstance(despatch_job(test_input), expected)
