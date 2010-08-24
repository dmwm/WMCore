"""
_UpdateStaus_

SQLite implementation of WorkQueueElement.UpdateStatus
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateStatus \
     import UpdateStatus as UpdateStatusMySQL

class UpdateStatus(UpdateStatusMySQL):
    queue_insert_sql = UpdateStatusMySQL.queue_insert_sql.replace('IGNORE', 'OR IGNORE')
