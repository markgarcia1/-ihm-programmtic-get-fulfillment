import json
import unittest
import warnings
# from app import create_app
from appConfig import AppConfig
from docDbRepo import DocDbRepo
from querybuilder import build_query

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
    "collection": "testCollection"

}

development = AppConfig.development
disable_parameter_validation = False


class TestConfig:
    config = AppConfig(None, development)
    config.manual_config(test_config['host'], test_config['dbName'], test_config['username'], test_config['password'],
                         test_config['collection'])
    config.environment = config.unit_test


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

    def test_insert(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        docId = repo.insert_one(appConfig.collectionName, mock_order)
        print("DocumentId = {}".format(docId))
        self.assertIsNotNone(docId)
        count = repo.get_collection_count(appConfig.collectionName)
        self.assertTrue(count == 5)
        doc = repo.find(appConfig.collectionName, {"orderId": "8808018297"})
        self.assertIsNotNone(doc)
        self.assertTrue(doc["orderId"] == "8808018297")
        self.assertTrue(doc["PageID"] == "2939293999999999")
        delete_count = repo.delete(appConfig.collectionName, {"PageID": "2939293999999999"})
        print("Repo.delete() returned : {}".format(delete_count))
        self.assertTrue(delete_count == 1)
        count = repo.get_collection_count(appConfig.collectionName)
        self.assertTrue(count == 4)
        return result

    def test_delete(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        docId = repo.insert_one(appConfig.collectionName, mock_order)
        print("DocumentId = {}".format(docId))
        self.assertIsNotNone(docId)
        count = repo.get_collection_count(appConfig.collectionName)
        self.assertTrue(count == 5)
        doc = repo.find(appConfig.collectionName, {"orderId": "8808018297"})
        self.assertIsNotNone(doc)
        self.assertTrue(doc["orderId"] == "8808018297")
        self.assertTrue(doc["PageID"] == "2939293999999999")
        delete_count = repo.delete(appConfig.collectionName, {"PageID": "2939293999999999"})
        print("Repo.delete() returned : {}".format(delete_count))
        self.assertTrue(delete_count == 1)
        count = repo.get_collection_count(appConfig.collectionName)
        self.assertTrue(count == 4)
        return result

    def test_find_by_pageId(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        repo.validate_parameters = disable_parameter_validation
        search_results = repo.find(appConfig.collectionName, {"PageID": "911"})
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
        search_results = repo.find(appConfig.collectionName, {"Station ID": "WGIR-FM"})
        print(search_results)
        self.assertIsNotNone(search_results)
        for doc in search_results:
            fulfillments = doc["fulfillmentList"]
            for f in fulfillments:
                self.assertTrue(f["Station ID"] == "WGIR-FM")

        return result

    def test_find_by_orderId(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        repo.validate_parameters = disable_parameter_validation
        doc = repo.find(appConfig.collectionName, {"orderId": "7800018297"})
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
        search_results = repo.find(appConfig.collectionName, {"startDate": "10/26/2019"})
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
        search_results = repo.find(appConfig.collectionName, query)
        print(search_results)
        self.assertIsNotNone(search_results)
        return result

    def test_find_by_start_and_end_date_range(self):
        result = False
        appConfig = AppConfig(None, development)
        appConfig.manual_config(test_config['host'], test_config['dbName'], test_config['username'],
                                test_config['password'], test_config['collection'])
        repo = DocDbRepo(appConfig)
        repo.validate_parameters = disable_parameter_validation
        criteria = {"startDate": "10/26/2019", "endDate": "11/30/2019"}
        search_results = repo.find(appConfig.collectionName, criteria)
        print(search_results)
        self.assertIsNotNone(len(search_results) > 0)
        return result


if __name__ == '__main__':
    unittest.main(verbosity=2)

# def main():
#     configResult = test_config()
#     print("Test Config result =" + str(configResult))
#     return
