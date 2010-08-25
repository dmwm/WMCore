"""
_GetElements_

MySQL implementation of WorkQueueElement.GetElements
"""

__all__ = []
__revision__ = "$Id: GetElements.py,v 1.9 2009/12/02 13:52:43 swakef Exp $"
__version__ = "$Revision: 1.9 $"

import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement as WQE


class GetElements(DBFormatter):
    sql = """SELECT we.id, we.status,  we.wmtask_id, we.input_id, we.num_jobs,
                    we.priority, we.parent_flag, we.insert_time,
                    we.update_time, we.subscription_id, we.parent_queue_id,
                    wq.url child_url, ww.url spec_url
                FROM wq_element we
                LEFT JOIN wq_queues wq ON we.child_queue = wq.id
				LEFT JOIN wq_wmtask wt ON we.wmtask_id = wt.id
                LEFT JOIN wq_wmspec ww ON wt.wmspec_id = ww.id
          """

    def formatWQE(self, data):
        """
        Take data and return as list of WorkQueueElements
        """
        result = []
        for item in data:
            result.append(WQE(Id = item['id'],
                              Status = States[item['status']],
                              Jobs = item['num_jobs'],
                              InsertTime = item['insert_time'],
                              UpdateTime = item['update_time'],
                              ChildQueueUrl = item['child_url'],
                              ParentQueueId = item['parent_queue_id'],
                              Priority = item['priority'],
                              SubscriptionId = item['subscription_id'],
                              WMSpecUrl = item['spec_url']))
        return result


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
        return self.formatWQE(self.formatDict(result))
