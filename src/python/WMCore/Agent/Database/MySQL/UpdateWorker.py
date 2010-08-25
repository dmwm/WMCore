"""
_InsertComponent_

MySQL implementation of UpdateWorker
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class UpdateWorker(DBFormatter):
    
    sqlpart1 = """UPDATE wm_workers 
                      SET last_updated = :last_updated  
               """ 
    sqlpart2 = """WHERE component_id = :component_id
                   AND name = :worker_name"""

    def execute(self, componentID, workerName, state = None,
                pid = None, conn = None, transaction = False):
        
        binds = {"component_id": componentID, 
                 "worker_name": workerName, 
                 "last_updated": int(time.time())}
                 
        if state:
            binds["state"] = state
            self.sqlpart1 += ", state = :state" 
        if pid:
            binds["pid"] = pid
            self.sqlpart1 += ", pid = :pid"
        
        sql = self.sqlpart1 + " " + self.sqlpart2
            
        self.dbi.processData(sql, binds, conn = conn,
                             transaction = transaction)
        return
