"""
_UpdateStaus_

SQLite implementation of WorkQueueElement.Delete
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.Delete import Delete \
     as DeleteMySQL

class Delete(DeleteMySQL):
    sql = DeleteMySQL.sql