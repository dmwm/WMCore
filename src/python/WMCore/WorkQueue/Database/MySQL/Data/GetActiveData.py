"""
_New_

MySQL implementation of Block.GetActiveBlocks
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class GetActiveData(DBFormatter):
    sql = """SELECT wb.id, name
                FROM wq_data wb
                INNER JOIN wq_element we ON we.input_id = wb.id
                WHERE we.status = :available
          """

    def execute(self, conn = None, transaction = False):
        binds = {'available' : States['Available']}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)
        return self.formatDict(results)
