import os
from base64 import b64decode


class AppConfig:
    info = "INFO"
    debug = "DEBUG"

    dev = "DEV"
    test = "TEST"
    prod = "PROD"
    unit_test = "UNIT_TEST"

    deploy = "Deploy"
    development = "Development"
    mode = deploy

    # class attribute
    host = 'DOCDB_HOST'
    userName = 'USER'
    userPassword = 'PASSWORD'
    dbName = 'DATABASE_NAME'
    connectionUrl = '';
    collectionName = ''
    uri = ''
    environment = dev
    logging_level = "INFO"

    def display_config(self) -> str:
        return ' "host" : "{}","database: "{}" ", "collection: "{}" ", "userName" : "{}", "userPassword" : "SHHH" ' \
            .format(self.host, self.dbName, self.collectionName, self.userName)

    def buildurl(self) -> str:
        # example MongoDb connection string.....
        # mongodb://dbadmin:<insertYourPassword>@programmatic-fulfillment.cluster-cgnzhhwlkuak.us-east-1.docdb.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false
        if self.mode == self.deploy:
            self.connectionUrl = 'mongodb://' + self.userName + ':' + self.userPassword + '@' + self.host + '/' \
                                 + self.dbName + '?replicaSet=rs0&readpreference=secondaryPreferred'
        elif self.mode == self.development:
            self.connectionUrl = "mongodb://dbadmin:ocdbadmin@localhost:27017/test"
        return self.connectionUrl

    def build_test_url(self):
        self.connectionUrl = "mongo " + self.host + " --username " + self.userName + " --password " + self.userPassword
        return self.connectionUrl

    # get all ENV proerties and set them
    def config(self):
        if self.mode is not self.development:
            try:
                self.host = os.environ['DB_HOST']
                self.dbName = os.environ['DB_NAME']
                self.userName = os.environ['DB_USERNAME']
                # encryptedPass = os.environ['DB_PASSWORD']
                # cipherTextBlob = b64decode(encryptedPass)
                # self.userPassword = passwd
                self.userPassword = os.environ['DB_PASSWORD']
                self.collectionName = os.environ['DB_COLLECTION_NAME']
                self.environment = os.environ['APP_ENVIRONMENT']
                self.logging_level = os.environ['APP_LOG_LEVEL']
                self.uri = self.buildurl()
                if self.logging_level == self.debug:
                    print("AppConfig: ENV HOST = " + str(self.host))
                    print("AppConfig: ENV DB_NAME = " + str(self.dbName))
                    print("AppConfig: ENV CollectionName = " + str(self.collectionName))
                    print("AppConfig: ENV Username = " + str(self.userName))
                    print("AppConfig: ENV App Log Level = " + str(self.logging_level))
                    print("AppConfig: ENV App Environment = " + str(self.environment))
                    print("AppConfig: DB Connection URI: " + self.buildurl())

            except Exception as e:
               print("App Configuration - Error processing Environment settings... {}".format(e))
               raise e
        return

    # manual configuration capability
    def manual_config(self, host, dbname, username, password, collection):
        self.mode = self.development
        self.environment = self.unit_test
        self.host = host
        print(str(self.host))
        self.dbName = dbname
        print(str(self.userName))
        self.userName = username
        self.userPassword = password
        self.collectionName = collection
        self.uri = self.build_test_url()
        return

    def __init__(self, context, mode):
        if mode is not None:
            self.mode = mode
        else:
            self.mode = self.deploy
        self.ctx = context;
        self.config()
