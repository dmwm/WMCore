"""
_GetRequestNamesByIDs_

MySQL implementation of WorkQueueElement.GetRequestNameByID
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class GetRequestNamesByIDs(DBFormatter):
    sql = """SELECT distinct(request_name) FROM wq_element
                WHERE %s = :id
          """

    def execute(self, ids, id_type, conn = None, transaction = False):
        self.sql = self.sql % id_type
        binds = [{'id': id} for id in ids]
        results = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        names = []
        if not results:
            return []
        for result in self.format(results):
            if result[0]:
                names.append(result[0])
        return names
