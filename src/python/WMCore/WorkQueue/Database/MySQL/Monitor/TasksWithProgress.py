"""
WMCore/WorkQueue/Database/MySQL/Monitor/WorkloadWithProgress.py


DAO object for WorkQueue

"""

__all__ = []
__revision__ = "$Id: TasksWithProgress.py,v 1.1 2010/05/20 21:15:53 sryu Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class TasksWithProgress(DBFormatter):
    sql = """SELECT wt.id as task_id, ws.name as spec_name, wt.name as task_name, 
                    url, owner, count(we.id) as total, 
                    (SELECT count(we.id) FROM wq_element we 
                     WHERE we.wmtask_id = wt.id AND we.status = :done)
                     as done  
             From wq_wmspec ws 
             INNER JOIN wq_wmtask wt ON (wt.wmspec_id = ws.id)
             INNER JOIN wq_element we ON (we.wmtask_id = wt.id) 
             GROUP BY wt.id
             ORDER BY wt.id"""
    
    def execute(self, conn = None, transaction = False):
        binds = {'done': States['Done']}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)
        
        return self.formatDict(results)