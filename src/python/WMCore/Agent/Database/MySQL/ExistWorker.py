"""
_ExistWorker_

MySQL implementation of ExistWorker
"""

__all__ = []
__revision__ = "$Id: ExistWorker.py,v 1.1 2010/06/21 21:19:17 sryu Exp $"
__version__ = "$Revision: 1.1 $"

import time
from WMCore.Database.DBFormatter import DBFormatter

class ExistWorker(DBFormatter):
    
    sql = """SELECT component_id FROM wm_workers 
                WHERE component_id = (SELECT id FROM wm_components 
                                   WHERE name = :component_name)
                AND name = :worker_name
             """
    
    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return result[0][0]
        
    def execute(self, componentName, workerName,
                pid = None, conn = None, transaction = False):
        
        binds = {"component_name": componentName, 
                 "worker_name": workerName}
        
        result = self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)
        return self.format(result)
    