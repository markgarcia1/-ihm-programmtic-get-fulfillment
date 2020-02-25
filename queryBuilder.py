import json
import datetime

from customExceptions import MissingParameterException, DateFormatException
from param import Parameter
from validators import validate_date_format

order_properties = ["PageID", "orderId", "startDate", "endDate", "billingCycle"]
startDate = 2
endDate = 3
validation_error_message = ""


# builds a query string for DocDbRepository
#  args:
#       input_parameter_list - list of key value pairs
#       validate_parameter_option - boolean flag to enable/disable parameter validation
def build_query(input_parameter_list):
    parameters = {}
    fulfillment_parameters = []
    paramKeys = [];
    query = ""
    try:
        for p in input_parameter_list:
            paramKeys.append(p)
            if p in order_properties:
                # print("{} is part of Order".format(p))
                if is_date_param(p) and validate_date_format(input_parameter_list[p]):
                    param = build_date_param(p, input_parameter_list[p])
                    parameters[p] = param[p]
                else:
                    # param = create_object(p, input_parameter_list[p])
                    parameters[p] = input_parameter_list[p]
            else:
                v = 'fulfillmentList.' + p
                param = create_object(v, input_parameter_list[p])
                parameters[v] = param[v]
    except DateFormatException as dfe:
        raise dfe
    except ValueError:
        pass
    except Exception as e:
        raise e
    return parameters


def validate_parameters(parameters_list):
    result = False
    if parameters_list.__contains__("PageId") and parameters_list.__contains__("orderId"):
        result = True
    return result


def build_param(key, value):
    return str(Parameter(key, value))


def build_date_param(key, value):
    if key == order_properties[startDate]:
        date_value = create_object(key, greater_than_or_equal_to(value))
    elif key == order_properties[endDate]:
        date_value = create_object(key, less_than_or_equal_to(value))
    else:
        date_value = create_object(key, value)
    return date_value


def is_date_param(key):
    if key == order_properties[startDate] or key == order_properties[endDate]:
        return True
    return False


def greater_than_or_equal_to(value):
    gte = create_object('$gte', value)
    return gte


def less_than_or_equal_to(value):
    lte = create_object('$lte', value)
    return lte


def create_object(key, value):
    return {key: value}
