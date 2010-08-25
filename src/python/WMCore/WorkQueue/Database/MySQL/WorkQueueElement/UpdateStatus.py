"""
_UpdateStaus_

MySQL implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: UpdateStatus.py,v 1.1 2009/06/26 21:06:23 sryu Exp $"
__version__ = "$Revision: 1.1 $"

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
