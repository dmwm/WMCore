"""
_InsertComponent_

MySQL implementation of UpdateWorker
"""

__all__ = []
__revision__ = "$Id: UpdateWorkerError.py,v 1.1 2010/06/23 18:07:05 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class UpdateWorkerError(DBFormatter):
    
    sql = """UPDATE wm_workers 
              SET last_error = :last_error,  
                  state = :state, error_message = :error_message 
              WHERE component_id = (SELECT id FROM wm_components WHERE name = :component_name)
                   AND name = :worker_name"""

    def execute(self, componentName, workerName, errorMessage,
                conn = None, transaction = False):
        binds = {"component_name": componentName, 
                 "worker_name": workerName, 
                 "last_error": int(time.time()),
                 "state": "Error",
                 "error_message": errorMessage}
        
        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return
