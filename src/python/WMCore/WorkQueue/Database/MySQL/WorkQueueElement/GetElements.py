"""
_GetElements_

MySQL implementation of WorkQueueElement.GetElements
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States
from WMCore.WorkQueue.DataStructs.WorkQueueElement import WorkQueueElement as WQE


class GetElements(DBFormatter):
    sql = """SELECT we.id, we.status,  wt.name, we.input_id, we.num_jobs,
                    we.priority, we.parent_flag, we.insert_time,
                    we.update_time, we.subscription_id, we.parent_queue_id,
                    we.events_written, we.files_processed, we.percent_complete,
                    we.percent_success, wq.url child_url, ww.url spec_url,
                    we.request_name, we.team_name
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
                              WMSpecUrl = item['spec_url'],
                              Task = item['name'],
                              EventsWritten = item['events_written'],
                              FilesProcessed = item['files_processed'],
                              PercentComplete = item['percent_complete'],
                              PercentSuccess = item['percent_success'],
                              RequestName = item['request_name'],
                              TeamName = item['team_name']))
        return result


    def execute(self, status = None,
                since = None, before = None, elementIDs=None,
                reqMgrUpdateNeeded = False,
                conn = None, transaction = False):
        binds = {}
        sep = "WHERE"
        if status is not None:
            binds['status'] = States[status]
            self.sql += " %s we.status = :status" % sep
            sep = "AND"
        if since is not None:
            binds['since'] = int(since)
            self.sql += " %s update_time >= :since" % sep
            sep = "AND"
        if before is not None:
            binds['before'] = int(before)
            self.sql += " %s update_time <= :before" % sep
            sep = "AND"
        if reqMgrUpdateNeeded:
            self.sql += " %s request_name IS NOT NULL AND reqmgr_time <= we.update_time" % sep
            sep = "AND"
        # must be last as it converts binds to a list
        if elementIDs:
            tmp_binds = []
            for id in elementIDs:
                bind = {'id': id}
                bind.update(binds)
                tmp_binds.append(bind)
            binds = tmp_binds
            self.sql += " %s we.id = :id" % sep
            sep = "AND"
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return self.formatWQE(self.formatDict(result))
