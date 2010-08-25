"""
_New_

MySQL implementation of WorkQueueElement.New
"""

__all__ = []
__revision__ = "$Id: New.py,v 1.4 2009/06/24 21:00:25 sryu Exp $"
__version__ = "$Revision: 1.4 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = """INSERT INTO wq_element (wmspec_id, block_id, num_jobs, priority,
                         parent_flag, insert_time) 
                 VALUES ((SELECT id FROM wq_wmspec WHERE name = :wmSpecName), 
                         (SELECT id FROM wq_block WHERE name = :blockName),  
                         :numJobs, :priority, :parentFlag, :insertTime)
          """

    def execute(self, wmSpecName, blockName, numJobs, priority, parentFlag = 0,
                conn = None, transaction = False):
        
        if blockName == None:
            blockName = "NoBlock"
        binds = {"wmSpecName":wmSpecName, "blockName":blockName, "numJobs":numJobs, 
                 "priority":priority, "parentFlag":parentFlag, 
                 "insertTime": int(time.time())}

        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)            
        return
