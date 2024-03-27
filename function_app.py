import azure.functions as func

import logging

# from src import despatch_job

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# @app.function_name(name="ocr")
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


# @app.route(route="ner", auth_level=func.AuthLevel.ANONYMOUS)
# def ner(req: func.HttpRequest) -> func.HttpResponse:

#     logging.info("Named entity recognition function invoked")
#     try:
#         response = despatch_job.despatch_job(req)
#         return response

#     except Exception as e:
#         error_msg = f"Failed: ner operation error {e}"
#         logging.error(error_msg)

#         return func.HttpResponse(error_msg)


@app.route(route="http_trigger", auth_level=func.AuthLevel.FUNCTION)
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    name = req.params.get("name")

    try:
        logging.info("OCR function invoked")
        try:
            from src import despatch_job

            response = despatch_job.despatch_job(req)
            return response

        except Exception as e:
            error_msg = f"Failed: OCR operation error {e}"
            logging.error(error_msg)

            return func.HttpResponse(error_msg)
    except Exception as e:
        logging.error(f"ERROR {e}")

    if name:
        return func.HttpResponse(
            f"Hello, {name}. This HTTP triggered function executed successfully."
        )
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            status_code=200,
        )
