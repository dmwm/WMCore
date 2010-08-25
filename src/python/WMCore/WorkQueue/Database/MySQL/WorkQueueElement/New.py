"""
_New_

MySQL implementation of Block.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.1 2009/06/10 21:07:16 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_element (wmspec_id, block_id, num_jobs, priority,
                         parent_flag, last_update) 
                 VALUES (:wmSpecID, :blockID, :numJobs, :priority, :parentFlag,
                         :lastUpdate)
          """

    def execute(self, wmSpecID, blockID, numJobs, priority, parentFlag = 0,
                conn = None, transaction = False):
        binds = {"wmSpecID":wmSpecID, "blockID":blockID, "numJobs":numJobs, 
                 "priority":priority, "parentFlag":parentFlag, 
                 "lastUpdate": int(time.time())}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
