"""
_GetElements_

MySQL implementation of WorkQueueElement.CountElements
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class CountElements(DBFormatter):
    sql = """SELECT COUNT(we.id)
                FROM wq_element we
          """
#                ORDER BY priority ASC
    def execute(self, status = None,
                since = None, before = None,
                conn = None, transaction = False):
        binds = {}
        sep = "WHERE"
        if status:
            binds['status'] = States[status]
            self.sql += "%s we.status = :status" % sep
            sep = "AND"
        if since:
            binds['since'] = since
            self.sql += "%s update_time >= :since" % sep
            sep = "AND"
        if before:
            binds['before'] = before
            self.sql += "%s update_time <= :before" % sep
            sep = "AND"
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return int(self.format(result)[0][0])
