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
                print("DocDbRepo: Delete successful for {}".format(criteria))
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
            collection = getattr(db, self.appConfig.collectionName)
            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: Inserting Document: {}".format(str(document)))
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
                print("DocDbRepo: Inserting multiple Documents: {}".format(str(data)))
            result = collection.insert_many(data)
            if not result:
                print("DocDbRepo: Insert_many() failed!")
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

    # Search for Documents by input criteria
    def find(self, criteria):
        try:
            print("DocDbRepo: Searching for Documents matching criteria: {}".format(str(criteria)))
            search_result = None
            client = self.open()
            results = []
            db = client.get_database(self.appConfig.dbName)
            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: DB = {}".format(db))

            collection = getattr(db, self.appConfig.collectionName)
            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: Collection = {}".format(collection))

            # Build search criteria query string and issue the find() on repo
            query = build_query(criteria)
            print("DocDbRepo: Calling find() for search criteria: {}".format(query))
            cursor = collection.find(query)
            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: Cursor: {}".format(str(cursor)))
                print("DocDbRepo: Cursor.count = {}".format(cursor.count()))

            # MAKE SURE we have data!!
            if cursor.count() != 0:
                for doc in cursor:
                    objId = str(doc["_id"])
                    doc["_id"] = objId
                    results.append(doc)
            else:
                print("DocDbRepo: Cursor had 0 elements.")

            #print("DocDbRepo: results = {} ".format(len(results)))
            self.close(client)

            if self.appConfig.logging_level == self.appConfig.debug:
                print("DocDbRepo: results[] count = {}".format(len(results)))

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
