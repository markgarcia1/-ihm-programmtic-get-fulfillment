import pymongo
import json
import sys

from querybuilder import build_query


class DocDbRepo:
    #  client = None;

    def __init__(self, config):
        self.appConfig = config;
        self.validate_parameters = True
        return

    # close existing connection
    def close(self, client):
        try:
            client.close()
        except Exception as ex:
            errno, strerror = ex.args
            print("Error({0}): {1}".format(errno, strerror))
            raise ex
        return

    # Open a connection to the database
    def open(self):
        print("Connecting to DocumentDB URI: {}".format(self.appConfig.buildurl()))
        return pymongo.MongoClient(self.appConfig.buildurl())

    # Delete document(s) by the specified criteria
    def delete(self, collection_name, criteria):
        try:
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            collection = db.get_collection(self.appConfig.collectionName)
            if self.appConfig.loging_level == self.appConfig.debug:
                print("Deleting Document matching criteria: {}".format(str(criteria)))
            result = collection.delete_one(criteria)
            count = -1
            if result is not None:
                # Do Something here to build message that the Document was deleted
                print("Delete successful. {}".format(criteria))
                count = result.deleted_count
            else:
                # Do something here to build message that the delete() failed!0
                print('delete failed for {}'.format(criteria))
            self.close(client)
            return count
        except Exception as ex:
            errno, strerror = ex.args
            print("Error({0}): {1}".format(errno, strerror))
            raise ex

    # Insert a single JSON Document into the database
    def insert_one(self, collection_name, document):
        try:
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            collection = db.get_collection(self.appConfig.collectionName)
            if self.appConfig.loging_level == self.appConfig.debug:
                print("Insert Document: {}".format(str(document)))
            objectId = collection.insert(document)
            if objectId is None:
                print("Insert for {} failed. ".format(document))
            self.close(client)
            return str(objectId)
        except Exception as ex:
            errno, strerror = ex.args
            print("Error({0}): {1}".format(errno, strerror))
            raise ex

    #  insert many Documents into collection
    def insert_many(self, collection_name, data: dict):
        try:
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            collection = db.get_collection(self.appConfig.collectionName)
            if self.appConfig.loging_level == self.appConfig.debug:
                print("Inserting multiple Documents: {}".format(str(criteria)))
            result = collection.insert_many(data)
            if not result:
                print("Insert_many() failed!")
            self.close(client)
            return len(result.inserted_ids)
        except Exception as ex:
            errno, strerror = ex.args
            print("Error({0}): {1}".format(errno, strerror))
            raise ex

    def findAll(self, collection):
        try:
            search_result = None
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            collection = db.get_collection(self.appConfig.collectionName)
            results = []
            cursor = collection.find()
            for doc in cursor:
                results.append(doc)

            # search_results = json.dumps(results, indent=0)
            self.close(client)
            return results
        except Exception as ex:
            errno, strerror = ex.args
            print("Error({0}): {1}".format(errno, strerror))
            raise ex

    # search for Documents matching input search criteria
    def find(self, collection_name, criteria):
        try:
            search_result = None
            client = self.open()
            results = []
            db = client.get_database(self.appConfig.dbName)
            collection = db.get_collection(self.appConfig.collectionName)
            if self.appConfig.loging_level == self.appConfig.debug:
                print("Find Documents matching criteria: {}".format(str(criteria)))
            query = build_query(criteria)
            cursor = collection.find(query)
            for doc in cursor:
                doc["_id"] = str(doc["_id"])
                results.append(doc)
            # search_result = json.dumps(results, indent=0)
            self.close(client)
            # Return the first Document in the list of we only have a single
            # Document, otherwise return the entire List<Document>
            return results[0] if len(results) == 1 else results
        except Exception as ex:
            errno, strerror = ex.args
            print("Error({0}): {1}".format(errno, strerror))
            raise ex

    # returns dictionary of collection names and counts
    def list_collection_names(self):
        try:
            list = []
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            collections = db.collection_names()
            for name in collections:
                list[str(name)] = db.get_collection(name).count()

            self.close(client)
            return list
        except Exception as ex:
            errno, strerror = ex.args
            print("Error({0}): {1}".format(errno, strerror))
            raise ex

    def get_collection_count(self, collection_name):
        try:
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            collection = db.get_collection(collection_name)
            count = collection.count()
            client.close()
            return count
        except Exception as ex:
            errno, strerror = ex.args
            print("Error({0}): {1}".format(errno, strerror))
            raise ex

    # # enable disable parameter validation
    # def set_parameter_validation(self, value):
    #     self.validate_parameters = value
    #     return self.validate_parameters
