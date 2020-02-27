import json
import unittest
import warnings
# from app import create_app
from appConfig import AppConfig
from docDbRepo import DocDbRepo
from queryBuilder import build_query, parameters_contain_start_and_end_dates, reorder_parameter_start_and_end_dates

unittest.TestLoader.sortTestMethodsUsing = None

mock_order = {
    "PageID": "2939293999999999",
    "orderId": "8808018297",
    "startDate": "01/1/2020",
    "endDate": "01/30/2020",
    "billingCycle": "January",
    "fulfillmentList": [
        {
            "Proposal Id": "7800018297",
            "Station ID": "WMAR-FM",
            "FCC ID": "35240",
            "Market": "Houston TX",
            "Advertiser": "Ownable",
            "Creative": "Gen Market Male V2 = RADIO",
            "PlaytimeGMT": "2019-12-02 05:00:36 GMT",
            "Duration": "29.962",
            "Plan Day Part": "mo-fr 12a-6a",
            "status": "Aired",
            "Price": "3.62",
            "Playtime Local":
                "2020-12-02 00:00:36 CST",
            "Campaign Impressions": "500",
            "Optimized Day Part": "mo-fr 12a-6a"
        }
    ]
}

test_config = {
    'host': "localhost:2707",
    'dbName': "test",
    "username": "dbadmin",
    "password": "ocdbadmin",
    "collection": "ProgFulfillment"

}

development = AppConfig.development
disable_parameter_validation = False


class TestConfig:
    config = AppConfig(None, development)
    config.manual_config(test_config['host'], test_config['dbName'], test_config['username'], test_config['password'],
                         test_config['collection'])
    config.environment = config.unit_test
    config.logging_level = config.debug


class RepTest(unittest.TestCase):
    config = None

    def setUp(self):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)
        # self.app = create_app( TestConfig)
        # self.app_context = self.app.app_context()
        # self.app_context.push()
        # db.create_all()

    def tearDown(self):
        print("teardown....")
        config = None
        return
        # db.session.remove()
        # db.drop_all()
        # self.app_context.pop()

    def test_config(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        appConfig.display_config()
        self.assertEqual(appConfig.host, test_config['host'])
        return result

    def test_connection(self):
        result = False
        self.assertTrue(False)
        return result

    def test_close_connection(self):
        result = False
        self.assertTrue(False)
        return result

    def test_collection_count(self):
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        count = repo.get_collection_count(appConfig.collectionName)
        self.assertTrue(count > 0)
        return count

    def test_find_all(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        search_results = repo.findAll(appConfig.collectionName)
        print(search_results)
        self.assertIsNotNone(search_results)
        return result

    # def test_insert(self):
    #     result = False
    #     appConfig = AppConfig(None, development)
    #     appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
    #                             test_config['password'], test_config['collection'])
    #     repo = DocDbRepo(appConfig)
    #     repoCount = repo.get_collection_count(appConfig.collectionName)
    #     docId = repo.insert_one(mock_order)
    #     print("DocumentId = {}".format(docId))
    #     self.assertIsNotNone(docId)
    #     count = repo.get_collection_count(appConfig.collectionName)
    #     self.assertTrue(count == repoCount+1)
    #     doc = repo.find({"orderId": "8808018297"})
    #     self.assertIsNotNone(doc)
    #     self.assertTrue(doc["orderId"] == "8808018297")
    #
    #     self.assertTrue(doc["PageID"] == "2939293999999999")
    #     delete_count = repo.delete({"PageID": "2939293999999999"})
    #     print("Repo.delete() returned : {}".format(delete_count))
    #     self.assertTrue(delete_count == 1)
    #     count = repo.get_collection_count(appConfig.collectionName)
    #     self.assertTrue(count == repoCount)
    #     return result
    #
    # def test_delete(self):
    #     result = False
    #     appConfig = AppConfig(None, development)
    #     appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
    #                             test_config['password'], test_config['collection'])
    #     repo = DocDbRepo(appConfig)
    #     repoCount = repo.get_collection_count(appConfig.collectionName)
    #
    #     docId = repo.insert_one(mock_order)
    #     print("DocumentId = {}".format(docId))
    #     self.assertIsNotNone(docId)
    #     count = repo.get_collection_count(appConfig.collectionName)
    #     self.assertTrue(count == repoCount+1)
    #     doc = repo.find({"orderId": "8808018297"})
    #     self.assertIsNotNone(doc)
    #     self.assertTrue(doc["orderId"] == "8808018297")
    #     self.assertTrue(doc["PageID"] == "2939293999999999")
    #     delete_count = repo.delete({"PageID": "2939293999999999"})
    #     print("Repo.delete() returned : {}".format(delete_count))
    #     self.assertTrue(delete_count == 1)
    #     count = repo.get_collection_count(appConfig.collectionName)
    #     self.assertTrue(count == repoCount)
    #     return result

    def test_find_by_pageId(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        repo.validate_parameters = disable_parameter_validation
        search_results = repo.find({"PageID": "911"})
        print(search_results)
        self.assertIsNotNone(search_results)

        return result

    def test_find_by_stationId(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        repo.validate_parameters = disable_parameter_validation
        search_results = repo.find({"Station ID": "WGIR-FM"})
        print(search_results)
        self.assertIsNotNone(search_results)
        for doc in search_results:
            fulfillments = doc["fulfillmentList"]
            for f in fulfillments:
                if "Station ID" in f.keys():
                    self.assertTrue(f["Station ID"] == "WGIR-FM")

        return result

    def test_find_by_orderId(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        repo.validate_parameters = disable_parameter_validation
        doc = repo.find({"orderId": "7800018297"})
        print(doc)
        self.assertIsNotNone(doc)
        self.assertTrue(doc["orderId"] == "7800018297")
        return result

    def test_find_by_startDate(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        repo.validate_parameters = disable_parameter_validation
        search_results = repo.find({"startDate": "10/26/2019"})
        print(search_results)
        self.assertIsNotNone(search_results)
        self.assertTrue(len(search_results) > 0)
        return result

    def test_find_by_endDate(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        repo.validate_parameters = disable_parameter_validation
        query = {"endDate": "11/30/2019"}
        search_results = repo.find(query)
        print("Results = {}".format(search_results))
        self.assertIsNotNone(search_results)
        return result

    def test_find_by_start_and_end_date_range(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        appConfig.logging_level = appConfig.debug
        repo = DocDbRepo(appConfig)
        repo.validate_parameters = disable_parameter_validation
        criteria = {"startDate": "10/26/2019", "endDate": "11/30/2019"}
        search_results = repo.find(criteria)
        print("REsults = {}".format(search_results))
        self.assertIsNotNone(len(search_results) > 0)
        return result

    # def test_reordering_of_start_and_end_dates(self):
    #     result = False
    #     criteria = {"endDate": "11/30/2019", "startDate": "10/26/2019"}
    #     print("Test Criteria = {}".format(criteria))
    #     result = parameters_contain_start_and_end_dates(criteria)
    #     print("contains start and end date returned {}".format(result))
    #     self.assertTrue(result)
    #     newCriteria = reorder_parameter_start_and_end_dates(criteria)
    #     print("reordering of criteria = {}".format(newCriteria))
    #     self.assertTrue(len(newCriteria.keys()) == 2)
    #     start = list(newCriteria.keys())[0]
    #     end = list(newCriteria.keys())[1]
    #     self.assertTrue(start == "startDate")
    #     self.assertTrue(end == "endDate")
    #     return result

    def test_startBillingDate(self):
         result = False
         appConfig = AppConfig(None, development)
         appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                 test_config['password'], test_config['collection'])
         repo = DocDbRepo(appConfig)
         repo.validate_parameters = disable_parameter_validation
         search_results = repo.find({"startBillingDate": "2019/11/01"})
         print(search_results)
         self.assertIsNotNone(search_results)
         self.assertTrue(len(search_results) > 0)
         return

if __name__ == '__main__':
    unittest.main(verbosity=2)
