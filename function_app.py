import azure.functions as func
import azure.durable_functions as df

import logging

from src import despatch_job


# function 1 call OCR on all files
dfApp = df.DFApp(http_auth_level=func.AuthLevel.ANONYMOUS)
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


# durable function 2 load model and call NER on all files
@dfApp.route(route="orchestrators/{functionName}")
@dfApp.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params.get("functionName")
    instance_id = await client.start_new(function_name)
    response = client.create_check_status_response(req, instance_id)
    return response


@dfApp.orchestration_trigger(context_name="context")
def hello_orchestrator(context):

    result1 = yield context.call_activity("hello", "Seattle")
    result2 = yield context.call_activity("hello", "Tokyo")
    result3 = yield context.call_activity("hello", "London")

    return [result1, result2, result3]


@dfApp.activity_trigger(input_name="city")
def hello(city: str):
    return f"Hello {city}"
