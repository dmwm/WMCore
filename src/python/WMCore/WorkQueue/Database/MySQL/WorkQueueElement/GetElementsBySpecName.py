"""
_GetElements_

MySQL implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElementsBySpecName.py,v 1.2 2009/07/06 20:23:36 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class GetElementsBySpecName(DBFormatter):
    sql = """SELECT we.id, we.wmspec_id, we.block_id, we.num_jobs, we.priority, 
                    we.parent_flag, we.insert_time, ws.status
                FROM wq_element we 
                    INNER JOIN wq_element_status ws ON (we.status = ws.id)
                    INNER JOIN wq_wmspec ww ON (ww.id = we.wmspec_id)
                WHERE ww.name = :specName
                ORDER BY priority ASC
          """

    def execute(self, specName, conn = None, transaction = False):
        binds = {"specName": specName}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return self.formatDict(result)
