"""
_New_

MySQL implementation of WorkQueueElement.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.5 2009/08/18 23:18:15 swakef Exp $"
__version__ = "$Revision: 1.5 $"

import time
from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class New(DBFormatter):
    sql = """INSERT INTO wq_element (wmspec_id, block_id, num_jobs, priority,
                         parent_flag, status, subscription_id, insert_time)
                 VALUES ((SELECT id FROM wq_wmspec WHERE name = :wmSpecName), 
                         (SELECT id FROM wq_block WHERE name = :blockName),  
                         :numJobs, :priority, :parentFlag, :available,
                         :subscription, :insertTime)
          """

    def execute(self, wmSpecName, blockName, numJobs, priority,
                parentFlag, subscription,
                conn = None, transaction = False):

        if blockName == None:
            blockName = "NoBlock"
        binds = {"wmSpecName" : wmSpecName, "blockName" : blockName,
                 "numJobs" : numJobs, "priority" : priority,
                 "parentFlag" : parentFlag, "insertTime" : int(time.time()),
                 "subscription" : subscription,
                 "available" : States['Available']}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
