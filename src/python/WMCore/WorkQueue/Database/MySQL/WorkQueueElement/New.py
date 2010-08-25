"""
_New_

MySQL implementation of WorkQueueElement.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.3 2009/06/19 22:14:56 sryu Exp $"
__version__ = "$Revision: 1.3 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_element (wmspec_id, block_id, num_jobs, priority,
                         parent_flag, insert_time) 
                 VALUES (:wmSpecID, :blockID, :numJobs, :priority, :parentFlag,
                         :insertTime)
          """

    def execute(self, wmSpecID, blockID, numJobs, priority, parentFlag = 0,
                conn = None, transaction = False):
        binds = {"wmSpecID":wmSpecID, "blockID":blockID, "numJobs":numJobs, 
                 "priority":priority, "parentFlag":parentFlag, 
                 "insertTime": int(time.time())}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
