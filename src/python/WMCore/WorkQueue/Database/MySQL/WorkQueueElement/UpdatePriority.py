"""
_UpdatePriority_

MySQL implementation of WorkQueueElement.UpdatePrioriy
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class UpdatePriority(DBFormatter):
    sql = """UPDATE wq_element SET priority = :priority
             WHERE wmtask_id IN (SELECT wt.id FROM wq_wmtask wt 
                                  INNER JOIN wq_wmspec ws ON ws.id = wt.wmspec_id 
                                  WHERE ws.name = :workflow)"""

    def execute(self, priority, workflows, conn = None, transaction = False):
        binds = [{"priority": priority, "workflow" : x} for x in workflows]
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return result[0].rowcount
