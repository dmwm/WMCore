"""
_UpdateStaus_

Oracle implementation of WorkQueueElement.UpdateStatus
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateStatus \
     import UpdateStatus as UpdateStatusMySQL

class UpdateStatus(UpdateStatusMySQL):
    
    queue_insert_sql = """INSERT INTO wq_queues (url) 
                            SELECT (:queue) FROM DUAL WHERE NOT EXISTS 
                            (SELECT * FROM wq_queues WHERE url = :queue)"""
    