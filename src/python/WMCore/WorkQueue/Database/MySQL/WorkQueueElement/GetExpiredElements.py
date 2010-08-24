"""
GetExpiredElements

MySQL implementation of WorkQueueElement.GetExpiredElements

Get elements that have been stuck in a state for too long
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class GetExpiredElements(DBFormatter):
    sql = """SELECT we.id, we.subscription_id, we.status, we.insert_time, we.update_time
             FROM wq_element we
             WHERE we.status = :status
          """

    def execute(self, status = 'Negotiating',
                since = None, before = None,
                interval = None,
                conn = None, transaction = False):

        binds = {'status' : States[status]}
        if since is not None:
            binds['since'] = since
            self.sql += " AND update_time >= :since"
        if before is not None:
            binds['before'] = before
            self.sql += " AND update_time <= :before"
        if interval is not None:
            binds['interval'] = interval
            binds['now'] = time.time()
            self.sql += " AND (:now - we.update_time) >= :interval"
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        result = self.formatDict(result)
        # replace status id's with human readable strings
        [x.__setitem__('status', States[x['status']]) for x in result]
        return result
