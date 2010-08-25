"""
_UpdateStaus_

Oracle implementation of WorkQueueElement.UpdateStatus
"""

__all__ = []
__revision__ = "$Id: UpdateStatus.py,v 1.8 2010/05/05 14:36:25 sryu Exp $"
__version__ = "$Revision: 1.8 $"

from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateStatus \
     import UpdateStatus as UpdateStatusMySQL

class UpdateStatus(UpdateStatusMySQL):
    
    queue_insert_sql = """INSERT INTO wq_queues (url) 
                            SELECT (:queue) FROM DUAL WHERE NOT EXISTS 
                            (SELECT * FROM wq_queues WHERE url = :queue)"""
    