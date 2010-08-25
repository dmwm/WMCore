"""
_GetElements_

MySQL implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElements.py,v 1.6 2009/11/12 16:43:31 swakef Exp $"
__version__ = "$Revision: 1.6 $"

import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class GetElements(DBFormatter):
    sql = """SELECT we.id, we.status, we.wmspec_id, we.input_id, we.num_jobs,
                    we.priority, we.parent_flag, we.insert_time,
                    we.update_time, we.subscription_id, we.parent_queue_id,
                    wq.url child_url
                FROM wq_element we
                LEFT JOIN wq_queues wq ON we.child_queue = wq.id
          """
#                ORDER BY priority ASC
    def execute(self, status = None,
                since = None, before = None, subs = None,
                conn = None, transaction = False):
        binds = {}
        sep = "WHERE"
        if status:
            binds['status'] = States[status]
            self.sql += "%s we.status = :status" % sep
            sep = "AND"
        if since:
            binds['since'] = int(since)
            self.sql += "%s update_time >= :since" % sep
            sep = "AND"
        if before:
            binds['before'] = int(before)
            self.sql += "%s update_time <= :before" % sep
            sep = "AND"
        if subs:
            tmp_binds = []
            for bind in subs:
                tmp = bind.copy()
                tmp['id'] = id
                tmp_binds.append(tmp)
            binds = tmp_binds
            self.sql += "%s subscription_id = :id" % sep
            sep = "AND"
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        result = self.formatDict(result)
        # replace status id's with human readable strings
        [x.__setitem__('status', States[x['status']]) for x in result]
        return result
