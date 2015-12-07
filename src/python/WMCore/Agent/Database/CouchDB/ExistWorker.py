"""
_ExistWorker_

CouchDB implementation of ExistWorker
"""

__all__ = []

import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Agent.Database.CouchDB.CouchService import CouchService

class ExistWorker(DBFormatter):

    def execute(self, componentName, workerName,
                conn = None, transaction = False):

        database = self.dbi.split("/")[len(self.dbi.split("/")) - 1]
        serverUrl = self.dbi.replace("/"+self.dbi.split("/")[len(self.dbi.split("/")) - 1],"")
        service = CouchService( serverUrl, database )

        query = {'key':componentName}
        worker_doc_id = ''

        res = service.load(query, 'Agent','existWorkers')
        if workerName in res[0]['value']:
            worker_doc_id = componentName

        return worker_doc_id
