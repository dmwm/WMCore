"""
WMCore/WorkQueue/Database/MySQL/Monitor/ElementsInfo.py

DAO object for WorkQueue

WorkQueue database structure:
WMCore/WorkQueue/Database/CreateWorkQueueBase.py

"""

__all__ = []
__revision__ = "$Id: ElementsInfoWithLimit.py,v 1.1 2010/05/10 22:06:40 sryu Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.Database.DBFormatter import DBFormatter
from WMCore.WorkQueue.Database import States

class ElementsInfoWithLimit(DBFormatter):
    """
    Use pagination (and synchronize with YUI table) 
    """
    sql = """SELECT we.id as id, ws.name as spec_name, wt.name as task_name, 
                    wd.name as element_name, parent_queue_id, child_queue, num_jobs,
                    priority, parent_flag, status, subscription_id, insert_time, update_time
             FROM wq_element we
             INNER JOIN wq_wmtask wt ON (wt.id = we.wmtask_id) 
             INNER JOIN wq_wmspec ws ON (ws.id = wt.wmspec_id)
             LEFT OUTER JOIN wq_data wd ON (wd.id = we.input_id)
             ORDER BY we.id
             LIMIT :startIndex, :results """
             
    def convertStatus(self, data):
        """
        Take data and convert status number to string
        TODO: overwrite formatDict to prevent this loop.
        """
        for item in data:
            item.update(status = States[item['status']])

    def execute(self, startIndex, results, conn = None, transaction = False):
        binds = {'startIndex': startIndex, 'results': results}
        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)
        
        formResults = self.formatDict(results) 
        self.convertStatus(formResults)
        return formResults