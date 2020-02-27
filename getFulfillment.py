import os
import json
from appConfig import AppConfig
from customExceptions import MissingParameterException, DateFormatException
from docDbRepo import DocDbRepo
from queryBuilder import build_query, parameters_contain_start_and_end_dates, reorder_parameter_start_and_end_dates
from response import Response
from validators import validate_get_parameters_not_null



def lambda_handler(event, context):
    results = []
    status_code = 200
    message = ""
    response = Response(500, "No message", None)
    try:
        print('JelliUpsertLambda: ENVIRONMENT VARIABLES: ')
        print(os.environ)
        print("JelliUpsertLambda: event = " + str(event))

        validate_get_parameters_not_null(event)
        config = AppConfig(context, None)

        # headers NOT WORKING!!!!
        #if config.logging_level == config.debug:
        #    print("JelliGetLambda: Headers = {}"+ event["headers"])
        print("JelliGetLambda: Connection URI: {}".format(config.buildurl()))
        repo = DocDbRepo(config)

        if repo is not None:
            if parameters_contain_start_and_end_dates(event["queryStringParameters"]):
                data = reorder_parameter_start_and_end_dates(event["queryStringParameters"])
                print("JelliGetLambda: reordered input parameter start and end dates....")
            else:
                data = event["queryStringParameters"]

            print("JelliGetLambda: input parameters: {}".format(data))
            # submit query
            values = repo.find(data)

            # process search results...
            if isinstance(values, list):
                for doc in values:
                    results.append(doc)
            else:
                results.append(values)

            rsp_data = results[0] if len(results) == 1 else results
            response = Response(200, rsp_data, "Query returned {} document(s).".format(len(results)))

            if config.logging_level == config.debug:
                print("JelliGetLambda: find query values = {}".format(values))
                print("JelliGetLambda: results List = {}".format(results))
                print("JelliGetLambda: response_data = {}".format(data))
                print("JelliGetLambda: response = {}".format(str(response)))
        else:
            print("JelliGetLambda: Error configuring Order Repository!")
            status_code = 500
            message = "JelliGetLambda: Error configuring Repository connection"
            response = Response(status_code, message, message)
        if repo is not None:
            print('JelliGetLambda: Finished. Closing repository object...')
            repo = None

    except DateFormatException as dfe:
        print("JelliGetLambda: Exception {}".format(dfe))
        print(dfe)
        response = Response(400, dfe.message, dfe.message)
    except MissingParameterException as mpe:
        print("JelliGetLambda: Exception {}".format(mpe))
        print(mpe)
        response = Response(400, mpe.message, mpe.message)
    except Exception as e:
        print("JelliGetLambda: Exception {}".format(e))
        status_code = 500
        message = "Unable to complete GET Request"
        response = Response(status_code, message, message)

    print("JelliGetLambda: Returning response = {}".format(response.toJSON()))
    return {
        "statusCode": response.status_code,
        "body": json.dumps(response.body)
    }
