"""
File       : MongoDB.py
Description: Provides a wrapper class for MongoDB
"""

# futures
from __future__ import division, print_function
from builtins import str, object

try:
    import mongomock
except ImportError:
    # this library should only be required by unit tests
    mongomock = None

from pymongo import MongoClient, errors, IndexModel
from pymongo.errors import ConnectionFailure


class MongoDB(object):
    """
    A simple wrapper class for creating a connection to a MongoDB instance
    """
    def __init__(self, database=None, server=None,
                 create=False, collections=None, testIndexes=False,
                 logger=None, mockMongoDB=False, **kwargs):
        """
        :databases:   A database Name to connect to
        :server:      The server url or a list of (server:port) pairs (see https://docs.mongodb.com/manual/reference/connection-string/)
        :create:      A flag to trigger a database creation (if missing) during
                      object construction, together with collections if present.
        :collections: A list of tuples describing collections with indexes -
                      the first element is considered the collection name, all
                      the rest elements are considered as indexes
        :testIndexes: A flag to trigger index test and eventually to create them
                      if missing (TODO)
        :mockMongoDB: A flag to trigger a database simulation instead of trying
                      to connect to a real database server.
        :logger:      Logger

        Here follows a short list of usefull optional parameters accepted by the
        MongoClient which may be passed as keyword arguments to the current module:

        :replicaSet:       The name of the replica set to connect to. The driver will verify
                           that all servers it connects to match this name. Implies that the
                           hosts specified are a seed list and the driver should attempt to
                           find all members of the set. Defaults to None.
        :port:             The port number on which to connect. It is overwritten by the ports
                           defined in the Url string or from the tuples listed in the server list
        :connect:          If True, immediately begin connecting to MongoDB in the background.
                           Otherwise connect on the first operation.
        :directConnection: If True, forces the client to connect directly to the specified MongoDB
                           host as a standalone. If False, the client connects to the entire
                           replica set of which the given MongoDB host(s) is a part.
                           If this is True and a mongodb+srv:// URI or a URI containing multiple
                           seeds is provided, an exception will be raised.
        :username:         A string
        :password:         A string
                           Although username and password must be percent-escaped in a MongoDB URI,
                           they must not be percent-escaped when passed as parameters. In this example,
                           both the space and slash special characters are passed as-is:
                           MongoClient(username="user name", password="pass/word")
        """
        self.server = server
        self.logger = logger
        self.mockMongoDB = mockMongoDB
        if mockMongoDB and mongomock is None:
            msg = "You are trying to mock MongoDB, but you do not have mongomock in the python path."
            self.logger.critical(msg)
            raise ImportError(msg)

        # NOTE: We need to explicitely check for server availiability.
        #       From pymongo Documentation: https://pymongo.readthedocs.io/en/stable/api/pymongo/mongo_client.html
        #       """
        #           ...
        #           Starting with version 3.0 the :class:`MongoClient`
        #           constructor no longer blocks while connecting to the server or
        #           servers, and it no longer raises
        #           :class:`~pymongo.errors.ConnectionFailure` if they are
        #           unavailable, nor :class:`~pymongo.errors.ConfigurationError`
        #           if the user's credentials are wrong. Instead, the constructor
        #           returns immediately and launches the connection process on
        #           background threads.
        #           ...
        #       """
        try:
            if mockMongoDB:
                self.client = mongomock.MongoClient()
                self.logger.info("NOTICE: MongoDB is set to use mongomock, instead of real database.")
            else:
                self.client = MongoClient(host=self.server, **kwargs)
            self.client.server_info()
            self.client.admin.command('ping')
        except ConnectionFailure as ex:
            msg = "Could not connect to MongoDB server: %s. Server not available. \n"
            msg += "Giving up Now."
            self.logger.error(msg, self.server)
            raise ex from None
        except Exception as ex:
            msg = "Could not connect to MongoDB server: %s. Due to unknown reason: %s\n"
            msg += "Giving up Now."
            self.logger.error(msg, self.server, str(ex))
            raise ex from None
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
        if db not in self.client.list_database_names():
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
            if not self.mockMongoDB:
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
                except Exception as exc:
                    msg = "Could not create MongoDB databases: %s\n%s\n" % (db, str(exc))
                    msg += "Giving up Now."
                    self.logger.error(msg)
                    raise exc
                try:
                    self._dbTest(db)
                except Exception as exc:
                    msg = "Second failure while testing %s\n%s\n" % (db, str(exc))
                    msg += "Giving up Now."
                    self.logger.error(msg)
                    raise exc
                msg = "Database %s successfully created" % db
                self.logger.error(msg)
        except Exception as ex:
            msg = "General Exception while trying to connect to : %s\n%s" % (db, str(ex))
            self.logger.error(msg)
            raise ex
