import json
import unittest
import warnings

from customExceptions import MissingParameterException, DateFormatException
from queryBuilder import build_query
from validators import validate_get_parameter_contains_orderId, validate_get_request

order_params = {
    "PageId": "123456",
    "orderId": "392829327",
    "ProposalId": "2323432",
    "startDate": "01/01/2019",
    "endDate": "05/30/2019",
    "billingCycle": "weekly"
}


class TestConfig:
    order = order_params


class RepTest(unittest.TestCase):
    config = None

    def setUp(self):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)

    def tearDown(self):
        print("teardown....")
        config = None
        return

    def test_get_by_pageId_fail(self):
        param = {"PageId": "123456"}
        try:
            query = build_query(param)
            print("Query = {} ".format(query))
            self.assertTrue(query is not None and query != "")
        except Exception as e:
            self.assertTrue(isinstance(e, MissingParameterException))
        return

    def test_get_by_orderId_fail(self):
        param = {"orderId": "123456"}
        try:
            query = build_query(param)
            print("Query = {} ".format(query))
            self.assertTrue(query is not None and query != "")
        except Exception as e:
            self.assertTrue(isinstance(e, MissingParameterException))
        return

    def test_get_valid_request_parameters(self):
        param = {"PageId": "123464", "orderId": "123456"}
        query = build_query(param)
        print("Query = {} ".format(query))
        self.assertTrue(query is not None and query != "")
        return

    def test_get_valid_startDate(self):
        param = {"startDate": "2020/02/18"}
        query = build_query(param)
        print("Query = {}".format(query))
        self.assertTrue("$gte" in str(query))
        return

    def test_invalid_startDate(self):
        param = {"startDate": "02-18-2020"}
        # self.assertRaises(ValueError, )
        with self.assertRaises(DateFormatException):
            build_query(param)
        return

    def test_get_request_contains_orderId_fail(self):
        param = {"queryStringParameters": {"startDate": "02-18-2020"}}
        err = "orderId is NULL or empty."
        try:
            validate_get_request(param)
            return False
        except MissingParameterException as mpe:
            self.assertTrue(mpe)
            self.assertTrue(mpe.status_code == 401)
            self.assertTrue(mpe.message == err)

        return True

    def test_get_request_contains_orderId(self):
        param = {"queryStringParameters": {"orderId": "12345", "startDate": "02-18-2020"}}
        err = "orderId is NULL or empty."
        try:
            value = validate_get_request(param)
            self.assertTrue(value is True)
        except MissingParameterException as mpe:
            print("Exception {}".format(mpe.message))
            raise mpe
            return False
        return True


if __name__ == '__main__':
    unittest.main(verbosity=2)
