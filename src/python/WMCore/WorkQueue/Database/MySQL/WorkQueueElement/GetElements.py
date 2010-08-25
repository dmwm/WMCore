"""
_New_

MySQL implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElements.py,v 1.2 2009/06/24 21:00:25 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class GetElements(DBFormatter):
    sql = """SELECT we.id, we.wmspec_id, we.block_id, we.num_jobs, we.priority, 
                    we.parent_flag, we.insert_time
                FROM wq_element we 
                    INNER JOIN wq_element_status ws ON (we.status = ws.id)
                WHERE ws.status = 'Available'
                ORDER BY priority ASC
          """

    def execute(self, conn = None, transaction = False):
        binds = {}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return self.formatDict(result)
