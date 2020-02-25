import json
import unittest
import warnings

from customExceptions import MissingParameterException, DateFormatException
from queryBuilder import build_query

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
        param = {"startDate": "02/18/2020"}
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
