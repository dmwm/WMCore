"""
_UpdateWorker_

NoSQL implementation of UpdateWorker
"""

__all__ = []


import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Agent.Database.CouchDB.CouchService import CouchService

class UpdateWorker(DBFormatter):


    def execute(self, componentID, workerName, state = None,
                pid = None, conn = None, transaction = False):


        database = self.dbi.split("/")[len(self.dbi.split("/")) - 1]
        serverUrl = self.dbi.replace("/"+self.dbi.split("/")[len(self.dbi.split("/")) - 1],"")
        service = CouchService(serverUrl, database)

        worker_doc = {}
        worker_doc = service.loadDoc(componentID)

        if worker_doc:

            worker_doc['workers'][workerName]['state']=state
            worker_doc['workers'][workerName]['last_updated'] = int(time.time())
            worker_doc['workers'][workerName]['pid']=pid

            service.addDoc(worker_doc)

        return
