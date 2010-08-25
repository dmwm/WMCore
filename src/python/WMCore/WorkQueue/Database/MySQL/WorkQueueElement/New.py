"""
_New_

MySQL implementation of WorkQueueElement.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.6 2009/09/03 15:44:20 swakef Exp $"
__version__ = "$Revision: 1.6 $"

import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class New(DBFormatter):
    sql = """INSERT INTO wq_element (wmspec_id, input_id, num_jobs, priority,
                         parent_flag, status, subscription_id, insert_time)
                 VALUES ((SELECT id FROM wq_wmspec WHERE name = :wmSpecName),
                         (SELECT id FROM wq_data WHERE name = :input),
                         :numJobs, :priority, :parentFlag, :available,
                         :subscription, :insertTime)
          """
    sql_no_input = """INSERT INTO wq_element (wmspec_id, num_jobs, priority,
                         parent_flag, status, subscription_id, insert_time)
                 VALUES ((SELECT id FROM wq_wmspec WHERE name = :wmSpecName),
                         :numJobs, :priority, :parentFlag, :available,
                         :subscription, :insertTime)
          """

    def execute(self, wmSpecName, input, numJobs, priority,
                parentFlag, subscription,
                conn = None, transaction = False):

        #if input == None:
        #    input = "NoBlock"
        binds = {"wmSpecName" : wmSpecName,
                 "numJobs" : numJobs, "priority" : priority,
                 "parentFlag" : parentFlag, "insertTime" : int(time.time()),
                 "subscription" : subscription,
                 "available" : States['Available']}
        if input:
            sql = self.sql
            binds['input'] = input
        else:
            sql = self.sql_no_input
        self.dbi.processData(sql, binds, conn = conn,
                             transaction = transaction)
        return
