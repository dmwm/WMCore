"""
_UpdatePriority_

SQLite implementation of WorkQueueElement.Priority
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdatePriority \
     import UpdatePriority as UpdatePriorityMySQL

class UpdatePriority(UpdatePriorityMySQL):
    sql = UpdatePriorityMySQL.sql
