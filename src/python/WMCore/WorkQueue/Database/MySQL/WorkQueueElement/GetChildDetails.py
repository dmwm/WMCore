"""
GetChildDetails

MySQL implementation of WorkQueueElement.GetChildDetails
"""

__all__ = []
__revision__ = "$Id: GetChildDetails.py,v 1.1 2009/11/17 16:53:36 swakef Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class GetChildDetails(DBFormatter):
    sql = """SELECT we.id, url
             FROM wq_element we, wq_queues
             WHERE we.child_queue_id = wq_queues.id
             AND we.subscription_id = :subscription
             ORDER BY url
          """

    def execute(self, subs, conn = None, transaction = False):
        binds = [{'subscription' : x} for x in subs]
        temp = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        temp = self.format(temp)
        result = {}
#        for url, ids in groupby(temp, itemgetter[0]):
#            result.append((url, tuple(lds)))
        for identifier, url in temp:
            try:
                result[url].append(identifier)
            except KeyError:
                result[url] = [identifier]
        return result
