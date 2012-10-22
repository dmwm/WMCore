"""
RESTSink - REST client to forward alerts to REST server.

Currently, the only REST server option is in fact CouchDB server.
This means that CouchSink and this RESTSink do the same job.
Should the necessity of a generic REST server arise in the future,
this RESTSink class shall be able to handle both such server
as well as CouchDB: then, alerts buffering, bulk posts, etc will
need to be implemented here taking a few necessary features
implemented along the chain
CMSCouch->CouchRequests->JSONRequests->Requests
rather than using only CMSCouch class directly.

"""

import logging

from WMCore.Database.CMSCouch import Document, Database, CouchServer



class RESTSink(object):
    """
    Alert sink for posting alerts to a REST server.
    The class acts as a REST client.

    """
    def __init__(self, config):
        # configuration values:
        #     'uri' attribute (URL of the REST server and resource name)
        #         in case of CouchDB, the resource name is the database name
        #         http://servername:port/databaseName
        self.config = config
        logging.info("Instantiating ...")

        # the class currently relies only on 1 REST server possibility - the
        # CouchDB server. as explained above, .database will be replaced by
        # .connection if both a generic REST server as well as CouchDB are to
        # be talked to
        split = self.config.uri.rfind('/')
        dbName = self.config.uri[split + 1:] # get last item of URI - database name
        url = self.config.uri[:split]
        # as opposed to CouchSink, here it's assumed the resource (the database name)
        # does exist, fail here otherwise
        # this check / rest of the constructed may be revised for
        #     general REST server
        server = CouchServer(url)
        databases = server.listDatabases()
        # there needs to be this database created upfront and also
        # couchapp associated with it installed, if it's there, fail
        if dbName not in databases:
            raise Exception("REST URI: %s (DB name: %s) does not exist." %
                            (self.config.uri, dbName))
        self._database = Database(dbName, url)
        logging.info("Initialized.")


    def send(self, alerts):
        """
        Send a list of alerts to a REST server.

        """
        for a in alerts:
            doc = Document(None, a)
            self._database.queue(doc)
        # two options here: either to call commit on the couch myself
        # or leave the alerts buffered in the Database queue which means
        # the .commit() would be called automatically if size is exceeded
        # 1st option:
        retVal = self._database.commit()
        logging.debug("Stored %s alerts to REST resource, retVals: %s" % (len(alerts), retVal))
        return retVal
