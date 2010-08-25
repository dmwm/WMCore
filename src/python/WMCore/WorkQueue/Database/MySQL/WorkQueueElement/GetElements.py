"""
_GetElements_

MySQL implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElements.py,v 1.5 2009/09/03 15:44:20 swakef Exp $"
__version__ = "$Revision: 1.5 $"

import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class GetElements(DBFormatter):
    sql = """SELECT we.id, we.wmspec_id, we.input_id, we.num_jobs, we.priority,
                    we.parent_flag, we.insert_time
                FROM wq_element we 
                WHERE we.status = :available
                ORDER BY priority ASC
          """

    def execute(self, conn = None, transaction = False):
        binds = {'available' : States['Available']}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return self.formatDict(result)
