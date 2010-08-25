"""
_GetHeartbeatInfo_

MySQL implementation of GetHeartbeatInfo
"""

__all__ = []
__revision__ = "$Id: GetAllHeartbeatInfo.py,v 1.2 2010/06/23 18:07:05 sryu Exp $"
__version__ = "$Revision: 1.2 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class GetAllHeartbeatInfo(DBFormatter):
    
    sql = """SELECT comp.name as name, comp.pid, worker.name as worker_name, 
                    worker.state, worker.last_updated, 
                    comp.update_threshold, worker.last_error, worker.error_message 
             FROM wm_workers worker
             INNER JOIN wm_components comp ON comp.id = worker.component_id
             """
    
    def execute(self, conn = None, transaction = False):
        
        result = self.dbi.processData(self.sql, conn = conn,
                             transaction = transaction)
        return self.formatDict(result)
    