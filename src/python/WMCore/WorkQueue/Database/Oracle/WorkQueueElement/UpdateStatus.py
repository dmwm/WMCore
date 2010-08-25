"""
_UpdateStaus_

Oracle implementation of WorkQueueElement.UpdateStatus
"""

__all__ = []
__revision__ = "$Id: UpdateStatus.py,v 1.7 2010/02/08 19:05:46 sryu Exp $"
__version__ = "$Revision: 1.7 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateStatus \
     import UpdateStatus as UpdateStatusMySQL

class UpdateStatus(UpdateStatusMySQL):
    
    queue_insert_sql = """INSERT INTO wq_queues (url) 
                            SELECT (:queue) FROM DUAL WHERE NOT EXISTS 
                            (SELECT * FROM wq_queues WHERE url = :queue)"""
    
    def execute(self, status, ids, id_type = 'id',
                child_queue = None,
                conn = None, transaction = False):
        UpdateStatusMySQL.execute(self, status, ids, id_type, child_queue, 
                                  conn, transaction)
        return True