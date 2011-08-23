"""
_UpdateWorkerError_

CouchDB implementation of UpdateWorker
"""

__all__ = []

import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Agent.Database.CouchDB.CouchService import CouchService

class UpdateWorkerError(DBFormatter):

    def execute(self, componentName, workerName, errorMessage,
                conn = None, transaction = False):

        database = self.dbi.split("/")[len(self.dbi.split("/")) - 1]
        serverUrl = self.dbi.replace("/"+self.dbi.split("/")[len(self.dbi.split("/")) - 1],"")
        service = CouchService(serverUrl, database)

        worker_doc = service.loadDoc(componentName)
        if worker_doc:

            worker_doc['workers'][workerName]['state']="Error"
            worker_doc['workers'][workerName]['error_message'] = errorMessage
            worker_doc['workers'][workerName]['last_error'] = int(time.time())

            service.addDoc(worker_doc)

        return
