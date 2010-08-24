"""
WMCore/WorkQueue/Database/MySQL/Monitor/WorkloadWithProgress.py


DAO object for WorkQueue

"""

__all__ = []




from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class WorkloadsWithProgress(DBFormatter):
    sql = """SELECT ws.id as spec_id, ws.name as spec_name, 
                    url, owner, count(we.id) as total, 
                    (SELECT count(we.id) FROM wq_element we
                     WHERE we.wmtask_id = wt.id AND we.status = :done)
                     as done  
             From wq_wmspec ws 
             INNER JOIN wq_wmtask wt ON (wt.wmspec_id = ws.id)
             INNER JOIN wq_element we ON (we.wmtask_id = wt.id) 
             GROUP BY ws.id
             ORDER BY ws.id"""
    
    def execute(self, conn = None, transaction = False):
        binds = {'done': States['Done']}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)
        
        return self.formatDict(results)