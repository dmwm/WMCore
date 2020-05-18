from pymongo import MongoClient, errors, IndexModel


class MongoDB(object):
    """
    A simple wrapper class for creating a connection to a MongoDB instance
    """
    def __init__(self,
                 database=None,
                 server=None,
                 port=None,
                 create=False,
                 collections=[],
                 testIndexes=False,
                 logger=None):
        """
        :databases:   A list of databases to connect to
        :create:      A flag to trigger a database creation (if missing) during
                      object construction, together with collections if present.
        :collections: A list of tuples describing collections with indexes -
                      the first element is considered the collection name, all
                      the rest elements are considered as indexes
        :testIndexes: A flag to trigger index test and eventually to create them
                      if missing (TODO)
        """
        # DONE:
        #    Database: msoutput
        #    To create two different collections for Relval and NonRelval
        # DONE:
        #    To read the configuration parameters (server, port) from service config
        self.server = server # '127.0.0.1'
        self.port = port # 8230
        try:
            self.client = MongoClient(self.server, self.port)
            self.client.server_info()
        except Exception as ex:
            msg = "ERR: could not connect to MongoDB server: %s\n%s" % (self.server, str(ex))
            msg += "Giving up Now."
            self.logger.error(msg)
            raise ex
        self.create = create
        self.testIndexes = testIndexes
        self.logger = logger
        self.dbName = database
        self.collections = collections

        self._dbConnect(database)

        if self.create and self.collections:
            for collection in self.collections:
                self._collCreate(collection, database)

        if self.testIndexes and self.collections:
            for collection in self.collections:
                self._indexTest(collection, database)

    def _indexTest(self, index):
        pass

    def _collTest(self, coll, db):
        # self[self.db].list_collection_names()
        pass

    def _collCreate(self, coll, db):
        """
        :coll: A tuple describing one collection with indexes -
               The first element is considered to be the collection name, and all
               the rest elements are considered to be indexes.
               The indexes must beof type IndexModel. See pymongo documentation:

               https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.create_index
        """
        # DONE:
        #     to create indexes on the proper fields

        collName = coll[0]
        collIndexes = list(coll[1:])
        try:
            self.client[db].create_collection(collName)
        except errors.CollectionInvalid:
            # this error is thrown in case of an already existing collection
            pass
        if collIndexes:
            # DONE:
            #    To implement a check for index types - should be either string or type IndexModel
            #    see: https://api.mongodb.com/python/current/api/pymongo/operations.html#pymongo.operations.IndexModel
            for index in collIndexes:
                if not isinstance(index, IndexModel):
                    msg = "ERR: Bad Index type for collection %s" % collName
                    raise errors.InvalidName
            try:
                self.client[db][collName].create_indexes(collIndexes)
            except Exception as ex:
                msg = "ERR: Failed to create indexes on collection: %s\n%s" % (collName, str(ex))
                self.logger.error(msg)
                raise ex

    def _dbTest(self, db):
        # Test connection (from mongoDB documentation):
        try:
            # The 'ismaster' command is cheap and does not require auth.
            self.client.admin.command('ismaster')
        except errors.ConnectionFailure as ex:
            msg = "Server not available: %s" % str(ex)
            self.logger.error(msg)
            raise ex

        # Test for database existence
        if db not in self.client.database_names():
            msg = "ERR: Missing MongoDB databases: %s" % db
            self.logger.error(msg)
            raise errors.InvalidName

    def _dbCreate(self, db):
        # creating an empty collection in order to create the database
        _initColl = self.client[db].create_collection('_initCollection')
        _initColl.insert_one({})
        # NOTE: never delete the _initCollection if you want the database to persist
        # self.client[db].drop_collection('_initCollection')

    def _dbConnect(self, db):
        try:
            setattr(self, db, self.client[db])
            self._dbTest(db)
        except errors.ConnectionFailure as ex:
            msg = "ERR: Could not connect to MongoDB server for database: %s\n%s\n" % (db, str(ex))
            msg += "Giving up Now."
            self.logger.error(msg)
            raise ex
        except errors.InvalidName as ex:
            msg = "ERR: Could not connect to a missing MongoDB databases: %s\n%s" % (db, str(ex))
            self.logger.error(msg)
            if self.create:
                msg = "Trying to create: %s" % db
                self.logger.error(msg)
                try:
                    # self._dbCreate(getattr(self, db))
                    self._dbCreate(db)
                except Exception as ex:
                    msg = "ERR: could not create MongoDB databases: %s\n%s\n" % (db, str(ex))
                    msg += "Giving up Now."
                    self.logger.error(msg)
                    raise ex
                try:
                    self._dbTest(db)
                except Exception as ex:
                    msg = "ERR: Second failure while testing %s\n%s\n" % (db, str(ex))
                    msg += "Giving up Now."
                    self.logger.error(msg)
                    raise(ex)
                msg = "Database %s successfully created" % db
                self.logger.error(msg)
        except Exception as ex:
            msg = "ERR: General Exception while trying to connect to : %s\n%s" % (db, str(ex))
            self.logger.error(msg)
            raise ex
