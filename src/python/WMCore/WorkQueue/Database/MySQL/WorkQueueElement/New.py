"""
_New_

MySQL implementation of WorkQueueElement.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.8 2009/11/12 16:43:31 swakef Exp $"
__version__ = "$Revision: 1.8 $"

import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class New(DBFormatter):
    sql = """INSERT INTO wq_element (wmspec_id, input_id, num_jobs, priority,
                         parent_flag, status, subscription_id, insert_time,
                         parent_queue_id)
                 VALUES ((SELECT id FROM wq_wmspec WHERE name = :wmSpecName),
                         (SELECT id FROM wq_data WHERE name = :input),
                         :numJobs, :priority, :parentFlag, :available,
                         :subscription, :insertTime, :parentQueueId)
          """
    sql_no_input = """INSERT INTO wq_element (wmspec_id, num_jobs, priority,
                         parent_flag, status, subscription_id, insert_time,
                         parent_queue_id, update_time)
                 VALUES ((SELECT id FROM wq_wmspec WHERE name = :wmSpecName),
                         :numJobs, :priority, :parentFlag, :available,
                         :subscription, :insertTime, :parentQueueId,
                         :insertTime)
          """

    def execute(self, wmSpecName, input, numJobs, priority,
                parentFlag, subscription, parentQueueId,
                conn = None, transaction = False):

        #if input == None:
        #    input = "NoBlock"
        binds = {"wmSpecName" : wmSpecName,
                 "numJobs" : numJobs, "priority" : priority,
                 "parentFlag" : parentFlag, "insertTime" : int(time.time()),
                 "subscription" : subscription,
                 "available" : States['Available'],
                 "parentQueueId" : parentQueueId}
        if input:
            sql = self.sql
            binds['input'] = input
        else:
            sql = self.sql_no_input
        self.dbi.processData(sql, binds, conn = conn,
                             transaction = transaction)
        return
