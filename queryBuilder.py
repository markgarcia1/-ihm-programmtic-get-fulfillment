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
    print("QueryBuilder: INPUT Parameters: {}".format(input_parameter_list))
    parameters = {}
    try:
        for p in input_parameter_list:
            p = p.replace('"', '')
            # print("QueryBuilder: Param = "+ p)
            if p in order_properties:
                # print("{} is part of Order".format(p))
                if is_date_param(p) and validate_date_format(input_parameter_list[p]):
                    param = build_date_param(p, input_parameter_list[p])
                    parameters[p] = param[p]
                else:

                    parameters[p] = input_parameter_list[p]
            else:
                # print("QueryBuilder: {} is a fulfillment property.".format(p))
                # append embedded doc qualifier to parameter key
                v = 'fulfillmentList.' + p
                param = create_object(v, input_parameter_list[p])
                parameters[v] = param[v]

        print("QueryBuilder: query = {}".format(str(parameters)))
    except DateFormatException as dfe:
        print("QueryBuilder: Exception {}".format(dfe))
        print(dfe)
        raise dfe
    except ValueError as ve:
        print("QueryBuilder: Exception {}".format(ve))
        print(ve)
        raise ve
    except Exception as ex:
        print("QueryBuilder: Exception {}".format(ex))
        print(ex)
        raise ex
    return parameters


# By default, API Gateway or Lambda automatically sorts GET parameters alphabetically.
# We need to reorder the parameter Map so that 'startDate' comes before 'endDate'.
def reorder_parameter_start_and_end_dates(parameters_dict):
    print("QueryBuilder: reordering 'starDate' and 'endDate' parameters.")
    print("QueryBuiler: original List = {}".format(parameters_dict))
    new_list = {}
    end_date = parameters_dict["endDate"]
    for key in parameters_dict:
        if key != "endDate":
            new_list[key] = parameters_dict[key]
        if key == "startDate":
            new_list["endDate"] = end_date
    return new_list


# Identify if Input GET Parameters contain both 'startDate' and 'endDate' arguments
def parameters_contain_start_and_end_dates(parameters_dict):
    result = False
    if parameters_dict.__contains__("startDate") and parameters_dict.__contains__("endDate"):
        result = True
    return result

# Validate input POST parameters to make sure they have 'PageID' and 'orderId' keys.
def validate_parameters(parameters_dict):
    result = False
    if parameters_dict.__contains__("PageId") and parameters_dict.__contains__("orderId"):
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
