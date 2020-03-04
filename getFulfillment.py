import os
import json
from appConfig import AppConfig
from customExceptions import MissingParameterException, DateFormatException
from docDbRepo import DocDbRepo
from queryBuilder import build_query, parameters_contain_start_and_end_dates, reorder_parameter_start_and_end_dates
from response import Response
from validators import validate_get_request


#TODO:
#     1) Implement better loggin solution for more granularity control of log messages
#     2) Refactor code to support Pagination of search results that exceed 6MB Response data limit for Lambda

def lambda_handler(event, context):
    results = []
    status_code = 200
    message = ""
    response = Response(500, "No message", None)
    try:
        print('progGetLambda: ENVIRONMENT VARIABLES: ')
        print(os.environ)
        print("progGetLambda: event = " + str(event))

        validate_get_request(event)
        config = AppConfig(context, None)

        repo = DocDbRepo(config)

        if repo is not None:
            if parameters_contain_start_and_end_dates(event["queryStringParameters"]):
                data = reorder_parameter_start_and_end_dates(event["queryStringParameters"])
                print("progGetLambda: reordered input parameter start and end dates....")
            else:
                data = event["queryStringParameters"]

            print("progGetLambda: input parameters: {}".format(data))
            # submit query
            results = repo.find(data)
            # build the response
            rsp_data = results[0] if len(results) == 1 else results
            response = Response(200, rsp_data, "Query returned {} document(s).".format(len(results)))

            if config.logging_level == config.debug:
                print("progGetLambda: results count = {}".format(len(results)))

        else:
            print("progGetLambda: Error configuring Order Repository!")
            status_code = 500
            message = "progGetLambda: Error configuring Repository connection"
            response = Response(status_code, message, message)
        if repo is not None:
            print('progGetLambda: Finished. Closing repository object...')
            repo = None

    except DateFormatException as dfe:
        print("progGetLambda: Exception {}".format(dfe))
        print(dfe)
        response = Response(400, dfe.message, dfe.message)
    except MissingParameterException as mpe:
        print("progGetLambda: Exception {}".format(mpe))
        print(mpe)
        response = Response(400, mpe.message, mpe.message)
    except Exception as e:
        print("progGetLambda: Exception {}".format(e))
        status_code = 500
        message = "Unable to complete GET Request"
        response = Response(status_code, message, message)

    return {
        "statusCode": response.status_code,
        "body": json.dumps(response.body)
    }
