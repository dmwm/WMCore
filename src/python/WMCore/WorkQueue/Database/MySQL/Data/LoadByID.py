"""
_New_

MySQL implementation of Block.LoadByID
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class LoadByID(DBFormatter):
    sql = """SELECT name
                FROM wq_data
                WHERE id = :data_id
          """

    def execute(self, data_id, conn = None, transaction = False):
        binds = {"data_id": data_id}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)
        return self.formatOneDict(results)
