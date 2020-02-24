import datetime

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
        if event is None:
            pde = PostDataException()
            pde.message = "POST requires at least a single document."
            print("Error: {}".format(pde))
            raise
        docs = event
        for doc in docs:
            if doc["PageID"] is None or doc["PageID"] == "":
                pde = PostDataException()
                pde.message = "Document PageID is Null or empty."
                print("Error: {}".format(pde))
                raise

    except Exception as e:
        print("Error:  {}".format(e))
        raise e
    return True


def validate_date_format(date_parameter):
    try:
        datetime.datetime.strptime(date_parameter, '%m/%d/%Y')
    except ValueError as ve:
        pass
        dfe = DateFormatException(ve)
        dfe.status_code = 401
        dfe.message = "Invalid date format for {}. Expected format: mm/dd/yyyy"
        dfe.body = dfe.message
        raise dfe
    return True
