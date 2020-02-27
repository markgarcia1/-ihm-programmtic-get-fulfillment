import datetime
import json

from customExceptions import MissingParameterException, DateFormatException, PostDataException


def validate_get_parameters_not_null(event):
    try:
        test = event["queryStringParameters"]
    except Exception as e:
        mpe = MissingParameterException(e)
        mpe.status_code = 401
        mpe.message = "GET requires at least one parameter and value per request."
        mpe.body = mpe.message
        raise mpe
    return True


def validate_post_data(event):
    try:
        print("Validators: validating POST DATA: " + event["body"])
        if isinstance(event, list):
            print("Validators:: event is a List object.")
        elif isinstance(event, dict):
            print("Validators:  event is a dictionary.")
        if event is None or "body" not in event.keys():
            pde = PostDataException()
            pde.message = "POST requires at least a single document."
            print("Validators: Error: {}".format(pde))
            raise
        post_data = event["body"]
        print("Validators: POST Data = " + json.dumps(post_data))
        if isinstance(post_data, list):
            print("Validators: POST data is a List.")
            for data in post_data:
                print("Validators:  Document = " + str(data))
                doc = json.loads(data)
                if "PageID" not in doc.keys() or doc["PageID"] == "":
                    pde = PostDataException()
                    pde.message = "Document PageID is Null or empty."
                    print("Validators:  Error: {}".format(pde))
                    raise
        else:
            if isinstance(post_data, dict):
                print("Validators:  POST data is a dictionary.")
            else:
                print("Validators:  POST data is not a dictionary.")
            doc = json.loads(post_data)
            if "PageID" not in doc.keys() or doc["PageID"] == "":
                pde = PostDataException()
                pde.message = "Document PageID is Null or empty."
                print("Validators:  Error: {}".format(pde))
                raise pde

    except Exception as e:
        print("Validators: Error:  {}".format(e))
        raise e
    return True


def validate_date_format(date_parameter):
    try:
        datetime.datetime.strptime(date_parameter, '%Y/%m/%d')
    except ValueError as ve:
        pass
        dfe = DateFormatException(ve)
        dfe.status_code = 401
        dfe.message = "Invalid date format for {}. Expected format: yyyy/mm/dd"
        dfe.body = dfe.message
        raise dfe
    return True
