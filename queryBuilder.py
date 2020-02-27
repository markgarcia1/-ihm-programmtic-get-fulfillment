from customExceptions import MissingParameterException, DateFormatException
from param import Parameter
from validators import validate_date_format

startDate = "startDate"
endDate = "endDate"
startDateDoc = "startDateDoc"
endDateDoc = "endDateDoc"
startBillingDate = "startBillingDate"
endBillingDate = "endBillingDate"

order_properties = ["PageID", "orderId", startDate, endDate, startDateDoc, endDateDoc, startBillingDate,
                    endBillingDate, "billingCycle"]

date_keys = [startDate, endDate, startDateDoc, endDateDoc, startBillingDate, endBillingDate]
end_date_keys = [endDate, endDateDoc, endBillingDate]
start_date_keys = [startDate, startDateDoc, startBillingDate]


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
            if p in order_properties:
                if is_date_param(p) and validate_date_format(input_parameter_list[p]):
                    param = build_date_param(p, input_parameter_list[p])
                    parameters[p] = param[p]
                else:

                    parameters[p] = input_parameter_list[p]
            else:
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
    print("QueryBuilder: reordering GET  start and end date parameters.")
    new_list = {}
    end_date_dict = build_end_date_dictionary(parameters_dict)
    for key in parameters_dict:
        if key not in end_date_keys:
            new_list[key] = parameters_dict[key]
            if key in start_date_keys:
                new_list = add_start_and_end_dates(new_list, key, end_date_dict)

    return new_list


# append start and end date key value pairs to the input List
def add_start_and_end_dates(list, key, end_date_dict):
    if key == startDate and end_date_dict[endDate] is not None:
        list[endDate] = end_date_dict[endDate]
    elif key == startDateDoc and end_date_dict[endDateDoc] is not None:
        list[endDateDoc] = end_date_dict[endDateDoc]
    elif key == startBillingDate and end_date_dict[endBillingDate] is not None:
        list[endBillingDate] = end_date_dict[endBillingDate]
    return list


# creates dictionary of end date key value pairs for
# sorting GET requests....
def build_end_date_dictionary(parameters_dict):
    dict = {}
    if endDate in parameters_dict.keys():
        dict[endDate] = parameters_dict[endDate]
    if endDateDoc in parameters_dict.keys():
        dict[endDateDoc] = parameters_dict[endDateDoc]
    if endBillingDate in parameters_dict.keys():
        dict[endBillingDate] = parameters_dict[endBillingDate]
    return dict


# Identify if Input GET Parameters contain both 'startDate' and 'endDate' arguments
def parameters_contain_start_and_end_dates(parameters_dict):
    result = False
    if (parameters_dict.__contains__(startDate) and parameters_dict.__contains__(endDate)) \
            or (parameters_dict.__contains__(startDateDoc) and parameters_dict.__contains__(endDateDoc)) \
            or (parameters_dict.__contains__(startBillingDate) and parameters_dict.__contains__(endBillingDate)):
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
    if key == start_date_keys[0] or key == start_date_keys[1] or key == start_date_keys[2]:
        date_value = create_object(key, greater_than_or_equal_to(value))
    elif key == end_date_keys[0] or key == end_date_keys[1] or key == end_date_keys[2]:
        date_value = create_object(key, less_than_or_equal_to(value))
    else:
        date_value = create_object(key, value)
    return date_value


def is_date_param(key):
    if (key == startDate) or (key == endDate) or (key == startDateDoc) or (key == endDateDoc) or (key == startBillingDate) or (key == endBillingDate):
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
