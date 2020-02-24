# define Python user-defined exceptions
class BaseErrorExcption(Exception):
    """Base class for other exceptions"""
    error_code = -1
    status_code = 400
    message = ""
    pass


class DateFormatException(BaseErrorExcption):
    error_code = 1000
    status_code = 401
    message = "Invalid Date Format. Expected mm-dd-yyyy"
    pass


class MissingParameterException(BaseErrorExcption):
    status_code = 401
    error_code = 1001
    message = "Empty Parameter queries are not accepted."
    pass


class PostDataException(BaseErrorExcption):
    error_code = 1002
    status_code = 401
    message = "POST method data invalid"
    pass


class RepositoryConnectionException(BaseErrorExcption):
    status_code = 500
    error_code = 1020
    message = "Unable to connect to DB Repository!"
    pass
