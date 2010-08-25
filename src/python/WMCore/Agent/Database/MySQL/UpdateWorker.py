"""
_InsertComponent_

MySQL implementation of UpdateWorker
"""

__all__ = []
__revision__ = "$Id: UpdateWorker.py,v 1.1 2010/06/21 21:19:19 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class UpdateWorker(DBFormatter):
    
    sql = """UPDATE wm_workers 
              SET last_updated = :last_updated,  
                  state = :state, pid = :pid 
              WHERE component_id = :component_id
                   AND name = :worker_name"""

    def execute(self, componentID, workerName, state = None,
                pid = None, conn = None, transaction = False):
        binds = {"component_id": componentID, 
                 "worker_name": workerName, 
                 "last_updated": int(time.time()),
                 "state": state,
                 "pid": pid}
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
