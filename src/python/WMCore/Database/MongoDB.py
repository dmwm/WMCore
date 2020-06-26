"""
File       : MongoDB.py
Description: Provides a wrapper class for MongoDB
"""

# futures
from __future__ import division, print_function

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
                 collections=None,
                 testIndexes=False,
                 logger=None):
        """
        :databases:   A database Name to connect to
        :server:      The server url (see https://docs.mongodb.com/manual/reference/connection-string/)
        :port:        Server port
        :create:      A flag to trigger a database creation (if missing) during
                      object construction, together with collections if present.
        :collections: A list of tuples describing collections with indexes -
                      the first element is considered the collection name, all
                      the rest elements are considered as indexes
        :testIndexes: A flag to trigger index test and eventually to create them
                      if missing (TODO)
        :logger:      Logger
        """
        self.server = server # '127.0.0.1'
        self.port = port # 8230
        self.logger = logger
        try:
            self.client = MongoClient(self.server, self.port)
            self.client.server_info()
        except Exception as ex:
            msg = "Could not connect to MongoDB server: %s\n%s" % (self.server, str(ex))
            msg += "Giving up Now."
            self.logger.error(msg)
            raise ex
        self.create = create
        self.testIndexes = testIndexes
        self.dbName = database
        self.collections = collections or []

        self._dbConnect(database)

        if self.create and self.collections:
            for collection in self.collections:
                self._collCreate(collection, database)

        if self.testIndexes and self.collections:
            for collection in self.collections:
                self._indexTest(collection[0], collection[1])

    def _indexTest(self, collection, index):
        pass

    def _collTest(self, coll, db):
        # self[db].list_collection_names()
        pass

    def collCreate(self, coll):
        """
        A public method for _collCreate
        """
        self._collCreate(coll, self.database)

    def _collCreate(self, coll, db):
        """
        A function used to explicitly create a collection with the relevant
        indexes - used to avoid the Lazy Creating from MongoDB and eventual issues
        in case we end up with no indexed collection, especially ones missing
        the (`unique` index parameter)
        :coll: A tuple describing one collection with indexes -
               The first element is considered to be the collection name, and all
               the rest of the elements are considered to be indexes.
               The indexes must be of type IndexModel. See pymongo documentation:

               https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.create_index

        :db:   The database name for the collection
        """

        collName = coll[0]
        collIndexes = list(coll[1:])
        try:
            self.client[db].create_collection(collName)
        except errors.CollectionInvalid:
            # this error is thrown in case of an already existing collection
            msg = "Collection '{}' Already exists in database '{}'".format(coll, db)
            self.logger.warning(msg)

        if collIndexes:
            for index in collIndexes:
                if not isinstance(index, IndexModel):
                    msg = "ERR: Bad Index type for collection %s" % collName
                    raise errors.InvalidName
            try:
                self.client[db][collName].create_indexes(collIndexes)
            except Exception as ex:
                msg = "Failed to create indexes on collection: %s\n%s" % (collName, str(ex))
                self.logger.error(msg)
                raise ex

    def _dbTest(self, db):
        """
        Tests database connection.
        """
        # Test connection (from mongoDB documentation):
        # https://api.mongodb.com/python/3.4.0/api/pymongo/mongo_client.html
        try:
            # The 'ismaster' command is cheap and does not require auth.
            self.client.admin.command('ismaster')
        except errors.ConnectionFailure as ex:
            msg = "Server not available: %s" % str(ex)
            self.logger.error(msg)
            raise ex

        # Test for database existence
        if db not in self.client.database_names():
            msg = "Missing MongoDB databases: %s" % db
            self.logger.error(msg)
            raise errors.InvalidName

    def _dbCreate(self, db):
        # creating an empty collection in order to create the database
        _initColl = self.client[db].create_collection('_initCollection')
        _initColl.insert_one({})
        # NOTE: never delete the _initCollection if you want the database to persist
        # self.client[db].drop_collection('_initCollection')

    def dbConnect(self):
        """
        A public method for _dbConnect
        """
        self._dbConnect(self.database)

    def _dbConnect(self, db):
        """
        The function to be used for the initial database connection creation and testing
        """
        try:
            setattr(self, db, self.client[db])
            self._dbTest(db)
        except errors.ConnectionFailure as ex:
            msg = "Could not connect to MongoDB server for database: %s\n%s\n" % (db, str(ex))
            msg += "Giving up Now."
            self.logger.error(msg)
            raise ex
        except errors.InvalidName as ex:
            msg = "Could not connect to a missing MongoDB databases: %s\n%s" % (db, str(ex))
            self.logger.error(msg)
            if self.create:
                msg = "Trying to create: %s" % db
                self.logger.error(msg)
                try:
                    # self._dbCreate(getattr(self, db))
                    self._dbCreate(db)
                except Exception as ex:
                    msg = "Could not create MongoDB databases: %s\n%s\n" % (db, str(ex))
                    msg += "Giving up Now."
                    self.logger.error(msg)
                    raise ex
                try:
                    self._dbTest(db)
                except Exception as ex:
                    msg = "Second failure while testing %s\n%s\n" % (db, str(ex))
                    msg += "Giving up Now."
                    self.logger.error(msg)
                    raise ex
                msg = "Database %s successfully created" % db
                self.logger.error(msg)
        except Exception as ex:
            msg = "General Exception while trying to connect to : %s\n%s" % (db, str(ex))
            self.logger.error(msg)
            raise ex
