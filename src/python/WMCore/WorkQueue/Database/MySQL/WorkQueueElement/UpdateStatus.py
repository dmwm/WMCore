"""
_UpdateStaus_

MySQL implementation of WorkQueueElement.UpdateStatus
"""

__all__ = []
__revision__ = "$Id: UpdateStatus.py,v 1.2 2009/08/12 16:59:49 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class UpdateStatus(DBFormatter):
    sql = """UPDATE wq_element SET status = 
              (SELECT id FROM wq_element_status WHERE status = :status)
          """

    def execute(self, status, conn = None, transaction = False):
        binds = {"status": status}
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return self.formatDict(result)
