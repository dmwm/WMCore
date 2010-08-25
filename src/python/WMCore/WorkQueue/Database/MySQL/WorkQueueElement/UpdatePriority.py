"""
_UpdatePriority_

MySQL implementation of WorkQueueElement.UpdatePrioriy
"""

__all__ = []
__revision__ = "$Id: UpdatePriority.py,v 1.2 2009/08/18 23:18:15 swakef Exp $"
__version__ = "$Revision: 1.2 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class UpdatePriority(DBFormatter):
    sql = """UPDATE wq_element SET priority = :priority
             WHERE wmspec_id = (SELECT id from wq_wmspec
                                   WHERE name = :workflow)"""

    def execute(self, priority, workflows, conn = None, transaction = False):
        binds = [{"priority": priority, "workflow" : x} for x in workflows]
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return result[0].rowcount
