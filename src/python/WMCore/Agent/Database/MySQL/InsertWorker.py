"""
_InsertComponent_

MySQL implementation of InsertWorker
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class InsertWorker(DBFormatter):
    
    sql = """INSERT INTO wm_workers (component_id, name, last_updated, state, pid) 
             VALUES ((SELECT id FROM wm_components WHERE name = :component_name), 
                     :worker_name, :last_updated, :state, :pid)
             """

    def execute(self, componentName, workerName, state = None,
                pid = None, conn = None, transaction = False):
        
        binds = {"component_name": componentName, 
                 "worker_name": workerName, 
                 "last_updated": int(time.time()),
                 "state": state, "pid": pid}
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
