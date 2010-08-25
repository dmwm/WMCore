"""
_GetHeartbeatInfo_

MySQL implementation of GetHeartbeatInfo
"""

__all__ = []



import time
from WMCore.Database.DBFormatter import DBFormatter

class GetHeartbeatInfo(DBFormatter):
    
    sql = """SELECT comp.name as name, comp.pid, worker.name as worker_name, 
                    worker.state, worker.last_updated, 
                    comp.update_threshold, worker.last_error, worker.error_message
             FROM wm_workers worker
             INNER JOIN wm_components comp ON comp.id = worker.component_id
             INNER JOIN (SELECT component_id, MAX(last_updated) AS last_updated FROM wm_workers
                         GROUP BY component_id) max_result
                        ON worker.last_updated = max_result.last_updated
             """
    #sql = """select max(last_updated) from wm_workers"""
    
    def execute(self, conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, conn = conn,
                             transaction = transaction)
        return self.formatDict(result)
    