"""
_InsertComponent_

MySQL implementation of Block.New
"""

__all__ = []

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.Agent.Database.CouchDB.CouchService import CouchService

class InsertComponent(DBFormatter):

    def execute(self, name, pid, update_threshold = 6000,
                conn = None, transaction = False):

        database = self.dbi.split("/")[len(self.dbi.split("/")) - 1]
        serverUrl = self.dbi.replace("/"+self.dbi.split("/")[len(self.dbi.split("/")) - 1],"")
        service = CouchService(serverUrl, database)

        doc = {}
        doc = service.loadDoc(name)

        if doc:
            service.delDoc(doc)

        wm_component = {'_id': name, 'workers' : {}, 'pid': pid, 'update_threshold': update_threshold}
        service.addDoc(wm_component)

        return
