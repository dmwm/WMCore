import WMCore.Database.CouchUtils as CouchUtils
import traceback
import logging

class CouchService(object):

    def __init__(self, url, database):
        """
        """
        self.url = url
        self.database = database
        self.server = None
        self.couchdb = None

    @CouchUtils.connectToCouch
    def load(self, query, design, view):
        """
        Load couch view.
        """
        result = ''
        result = self.couchdb.loadView(design, view, query)['rows']
        return result

    @CouchUtils.connectToCouch
    def loadDoc(self, docId):
        """
        Load document from couch.
        """
        result = ''
        try:
            result = self.couchdb.document(docId)
        except Exception, ex:
            msg =  "Error loading document from couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
        return result

    @CouchUtils.connectToCouch
    def delDoc(self, doc):
        """
        Remove document from couch.
        """
        try:
            self.couchdb.queueDelete(doc)
        except Exception, ex:
            msg =  "Error queuing document for delete from couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)

        try:
            self.couchdb.commit()
        except Exception, ex:
            msg =  "Error commiting documents in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)

    @CouchUtils.connectToCouch
    def addDoc(self, doc):
        """
        Add document in couch.
        """
        try:
            self.couchdb.queue(doc)
        except Exception, ex:
            msg =  "Error queuing document into couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)

        try:
            self.couchdb.commit()
        except Exception, ex:
            msg =  "Error commiting document in couch"
            msg += str(ex)
            msg += str(traceback.format_exc())
            logging.error(msg)
