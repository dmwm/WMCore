"""
CouchSink - CouchDB alerts store with the AlertProcessor.

"""

import logging

from WMCore.Database.CMSCouch import Document, Database, CouchServer


class CouchSink(object):
    """
    Alert sink for pushing alerts to a couch database.

    """
    def __init__(self, config):
        self.config = config
        logging.info("Instantiating ...")
        # test if the configured database does not exist, create it
        server = CouchServer(self.config.url)
        databases = server.listDatabases()
        if self.config.database not in databases:
            logging.warn("'%s' database does not exist on %s, creating it ..." %
                         (self.config.database, self.config.url))
            server.createDatabase(self.config.database)
            logging.warn("Created.")
        logging.info("'%s' database exists on %s" % (self.config.database, self.config.url))
        self.database = Database(self.config.database, self.config.url)
        logging.info("Initialized.")


    def send(self, alerts):
        """
        Handle list of alerts.

        """
        retVals = []
        for a in alerts:
            doc = Document(None, a)
            retVal = self.database.commitOne(doc)
            retVals.append(retVal)
        logging.debug("Stored %s alerts to CouchDB, retVals: %s" % (len(alerts), retVals))
        return retVals
