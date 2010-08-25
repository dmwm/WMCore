"""
_GetParentId_

MySQL implementation of WorkQueueElement.GetParentId
"""

__all__ = []
__revision__ = "$Id: GetParentId.py,v 1.1 2009/09/17 15:37:53 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class GetParentId(DBFormatter):
    sql = """SELECT we.parent_queue_id
                FROM wq_element we
                WHERE we.id = :id
          """

    def execute(self, ids, conn = None, transaction = False):
        binds = [{'id' : x} for x in ids]
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return self.formatDict(result)
