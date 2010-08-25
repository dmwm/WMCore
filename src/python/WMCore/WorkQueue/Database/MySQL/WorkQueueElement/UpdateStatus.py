"""
_UpdateStaus_

MySQL implementation of WorkQueueElement.UpdateStatus
"""

__all__ = []
__revision__ = "$Id: UpdateStatus.py,v 1.3 2009/08/18 23:18:15 swakef Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class UpdateStatus(DBFormatter):
    sql = """UPDATE wq_element SET status = :status
              WHERE subscription_id = :subscription
          """

    def execute(self, status, subscriptions, conn = None, transaction = False):
        if status not in States:
            raise RuntimeError, "Invalid state: %s" % status

        binds = [{"status": States[status],
                  "subscription" : x} for x in subscriptions]
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return result[0].rowcount
