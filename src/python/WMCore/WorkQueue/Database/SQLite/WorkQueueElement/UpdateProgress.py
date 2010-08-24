"""
_UpdateProgress_

SQLite implementation of WorkQueueElement.UpdateProgress
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateProgress \
     import UpdateProgress as UpdateProgressMySQL

class UpdateProgress(UpdateProgressMySQL):
    pass
