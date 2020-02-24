import json
import pymongo
import json
import pymongo
from appConfig import AppConfig
from customExceptions import MissingParameterException, DateFormatException
from docDbRepo import DocDbRepo
from querybuilder import build_query
from response import Response
from validators import validate_get_parameters_not_null


def lambda_handler(event, context):
        results = []
        status_code = 200
        message = ""
        response = None
        try:
            validate_get_parameters_not_null(event)
            config = AppConfig(context, None)
            repo = DocDbRepo(config)

            if repo is not None:
                # build query criteria
                query = build_query(event["getStringParameters"])
                # submit query
                cursor = repo.find(config.collectionName, query)
                print("iterating cursor")
                for doc in cursor:
                    results.append(doc)

                data = results[0] if len(results)==1 else results
                response = Response(200, data, None)
            else:
                print("Error configuring Order Repository!")
                status_code = 500
                message = "Error configuring Repository connection"
                response = Response(status_code, message, message)
            if repo is not None:
                print('closing repo...')
                repo = None

        except DateFormatException as dfe:
            response = Response(dfe.status_code, dfe.message, dfe.message)
        except MissingParameterException as mpe:
            response = Response(mpe.status_code, mpe.message, mpe.message)
        except Exception as e:
            print("Exception {}".format(e))
            status_code = 500
            message = "Unable to complete GET Request"
            response = Response(status_code, message, message)

        return {
            "statusCode": response.status_code,
            "body": response.body
        }
