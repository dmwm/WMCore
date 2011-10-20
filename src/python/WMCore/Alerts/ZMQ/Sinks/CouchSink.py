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
        # test if the configured database does not exist, create it
        server = CouchServer(self.config.url)
        databases = server.listDatabases()
        if self.config.database not in databases:
            server.createDatabase(self.config.database)
        self.database = Database(self.config.database, self.config.url)
        logging.debug("%s initialized." % self.__class__.__name__)
        
        
    def send(self, alerts):
        """
        Handle list of alerts.
        
        """
        retVals = []
        for a in alerts:
            doc = Document(None, a)
            retVal = self.database.commitOne(doc)
            retVals.append(retVal)
        logging.debug("%s stored alerts, retVals: %s" % (self.__class__.__name__, retVals))
        return retVals