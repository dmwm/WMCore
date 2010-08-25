"""
_UpdatePriority_

MySQL implementation of WorkQueueElement.UpdatePrioriy
"""

__all__ = []
__revision__ = "$Id: UpdatePriority.py,v 1.1 2009/08/12 16:59:49 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class UpdatePriority(DBFormatter):
    sql = """UPDATE wq_element SET priority = :priority
          """

    def execute(self, priority, conn = None, transaction = False):
        binds = {"priority": priority}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return self.formatDict(result)
