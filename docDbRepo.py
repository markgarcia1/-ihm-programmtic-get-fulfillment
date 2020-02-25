import pymongo
from queryBuilder import build_query


class DocDbRepo:
    #  client = None;

    def __init__(self, config):
        self.appConfig = config;
        self.validate_parameters = True
        return

    # close existing connection
    def close(self, client):
        try:
            print("DocDbRepo: Close client")
            client.close()
            print("DocDbRepo: closed client")
        except Exception as ex:
            errno, strerror = ex.args
            print("Error closing client ({0}): {1}".format(errno, strerror))
            raise ex
        return

    # Open a connection to the database
    def open(self):
        print("Connecting to DocumentDB URI: {}".format(self.appConfig.buildurl()))
        return pymongo.MongoClient(self.appConfig.buildurl())

    # Delete document(s) by the specified criteria
    def delete(self, criteria):
        try:
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            collection = getattr(db, self.appConfig.collectionName)

            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: DB = {}".format(db))
                print("DocDbRepo: DB Collection = {}".format(collection))
                print("DocDbRepo: Deleting Document matching criteria: {}".format(str(criteria)))
            result = collection.delete_one(criteria)
            count = -1
            if result is not None:
                # Do Something here to build message that the Document was deleted
                print("DocDbRepo: Delete successful for  {}".format(criteria))
                count = result.deleted_count
            else:
                # Do something here to build message that the delete() failed!0
                print('DocDbRepo: Delete failed for {}'.format(criteria))
            self.close(client)
            return count
        except Exception as ex:
            print("DocDbRepo: Exception: {}".format(ex))
            print(ex)
            raise ex

    # Insert a single JSON Document into the database
    def insert_one(self, document):
        try:
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            # collection = db.get_collection(self.appConfig.collectionName)
            collection = getattr(db, self.appConfig.collectionName)
            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: Insert Document: {}".format(str(document)))
            objectId = collection.insert(document)
            if objectId is None:
                print("DocDbRepo: Insert failed for document:  ".format(document))
                objectId = -9999
            self.close(client)
            return str(objectId)
        except Exception as ex:
            print("DocDbRepo: Exception: {}".format(ex))
            print(ex)
            raise ex

    #  insert many Documents into collection
    def insert_many(self, data: dict):
        try:
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            collection = db.get_collection(self.appConfig.collectionName)
            # collection = getattr(db, self.appConfig.collectionName)
            if self.appConfig.logging_level == self.appConfig.debug:
                print("Inserting multiple Documents: {}".format(str(data)))
            result = collection.insert_many(data)
            if not result:
                print("Insert_many() failed!")
            self.close(client)
            return len(result.inserted_ids)
        except Exception as ex:
            print("DocDbRepo: Exception: {}".format(ex))
            print(ex)
            raise ex

    def findAll(self, collection):
        try:
            search_result = None
            client = self.open()
            db = client.get_database(self.appConfig.dbName)
            # collection = db.get_collection(self.appConfig.collectionName)
            collection = getattr(db, self.appConfig.collectionName)
            results = []
            cursor = collection.find()
            for doc in cursor:
                results.append(doc)

            # search_results = json.dumps(results, indent=0)
            self.close(client)
            return results
        except Exception as ex:
            print("DocDbRepo: Exception: {}".format(ex))
            print(ex)
            raise ex

    # search for Documents matching input search criteria
    # def find(self, criteria):
    #     try:
    #         print("DocDbRepo: Searching for Documents matching criteria: {}".format(str(criteria)))
    #         search_result = None
    #         client = self.open()
    #         results = []
    #         db = client.get_database(self.appConfig.dbName)
    #         if self.appConfig.logging_level == self.appConfig.debug:
    #             print("DocDbRepo: DB = {}".format(db))
    #             print("DocDbRepo: collectionName = {}".format(self.appConfig.collectionName))
    #
    #         collection = getattr(db, self.appConfig.collectionName)
    #
    #         if self.appConfig.logging_level == self.appConfig.debug:
    #             print("DocDbRepo: got collection....")
    #             print("DocDbRepo: Collection = {}".format(collection))
    #
    #         query = build_query(criteria)
    #         print("DocDbRepo: searching for Orders by {}".format(query))
    #         cursor = collection.find(query)
    #         if self.appConfig.logging_level == self.appConfig.debug:
    #             print("Cursor: {}".format(str(cursor)))
    #
    #         for doc in cursor:
    #             print("Doc: {}".format(doc))
    #             objId = str(doc["_id"])
    #             print("ObjectID: {}".format(objId))
    #             doc["_id"] = objId
    #             print("DocDbRepo: appending document to List...")
    #             results.append(doc)
    #
    #         print("DocDbRepo: results has {} documents".format(len(results)))
    #         print("DocDbRepo: attempting to close client")
    #         self.close(client)
    #
    #         if self.appConfig.logging_level == self.appConfig.debug:
    #             if len(results) == 1:
    #                 print("DocDbRepo: results List has 1 item")
    #                 print("DocDbRepo: item[0] = {}".format(results[0]))
    #             else:
    #                 print("DocDbRepo: results List has multiple items")
    #                 print("DocDbRepo: item[] = {}".format(results))
    #         # Return the first Document in the list of we only have a single
    #         # Document, otherwise return the entire List<Document>
    #         return results[0] if len(results) == 1 else results
    #     except Exception as ex:
    #         errno, strerror = ex.args
    #         print("Error({0}): {1}".format(errno, strerror))
    #         raise ex
    def find(self, criteria):
        try:
            print("DocDbRepo: Searching for Documents matching criteria: {}".format(str(criteria)))
            search_result = None
            client = self.open()
            results = []
            db = client.get_database(self.appConfig.dbName)
            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: DB = {}".format(db))
                print("DocDbRepo: collectionName = {}".format(self.appConfig.collectionName))

            collection = getattr(db, self.appConfig.collectionName)

            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: got collection....")
                print("DocDbRepo: Collection = {}".format(collection))

            query = build_query(criteria)
            print("DocDbRepo: find() query = {}".format(query))
            cursor = collection.find(query)
            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: Cursor: {}".format(str(cursor)))

            if cursor.count() != 0:
                for doc in cursor:
                    # print("Doc: {}".format(doc))
                    objId = str(doc["_id"])
                    # print("ObjectID: {}".format(objId))
                    doc["_id"] = objId
                    # print("DocDbRepo: appending document to List...")
                    results.append(doc)
            else:
                print("DocDbRepo: Cursor had 0 elements.")

            print("DocDbRepo: results has {} documents".format(len(results)))
            print("DocDbRepo: calling Close on DB client")
            self.close(client)

            if self.appConfig.logging_level == self.appConfig.debug:
                if len(results) == 1:
                    print("DocDbRepo: results List has 1 item")
                    print("DocDbRepo: item[0] = {}".format(results[0]))
                else:
                    print("DocDbRepo: results List has multiple items")
                    print("DocDbRepo: item[] = {}".format(results))
            # Return the first Document in the list of we only have a single
            # Document, otherwise return the entire List<Document>
            return results[0] if len(results) == 1 else results
        except Exception as ex:
            print("DocDbRepo: Exception: {}".format(ex))
            print(ex)
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
            print("DocDbRepo: Exception: {}".format(ex))
            print(ex)
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
            print("DocDbRepo: Exception: {}".format(ex))
            print(ex)
            raise ex
    # # enable disable parameter validation
    # def set_parameter_validation(self, value):
    #     self.validate_parameters = value
    #     return self.validate_parameters
