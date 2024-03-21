import azure.functions as func
import logging

# from despatch_job import despatch_job

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


# @app.route(route="ocr", auth_level=func.AuthLevel.FUNCTION)
# def ocr(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info("OCR function invoked")
#     try:
#         response = despatch_job(req)
#         return response

#     except Exception as e:
#         logging.error("An error occurred invoking OCR operation: {e}")


@app.route(route="named_entity_recognition", auth_level=func.AuthLevel.FUNCTION)
def named_entity_recognition(req: func.HttpRequest) -> func.HttpResponse:

    logging.info("NER placeholder")

    return func.HttpResponse("True")


@app.route(route="ocr", auth_level=func.AuthLevel.FUNCTION)
def ocr(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )