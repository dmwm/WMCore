"""
_InsertWorker_

CouchDB implementation of InsertWorker
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Agent.Database.CouchDB.CouchService import CouchService

class InsertWorker(DBFormatter):

    def execute(self, componentName, workerName, state = None,
                pid = None, conn = None, transaction = False):

        database = self.dbi.split("/")[len(self.dbi.split("/")) - 1]
        serverUrl = self.dbi.replace("/"+self.dbi.split("/")[len(self.dbi.split("/")) - 1],"")
        service = CouchService(url = serverUrl, database = database)

        doc = service.loadDoc(componentName)
        doc['workers'][workerName] = { 'last_updated': int(time.time()), 'state': state, "pid": pid }

        service.addDoc(doc)

        return
