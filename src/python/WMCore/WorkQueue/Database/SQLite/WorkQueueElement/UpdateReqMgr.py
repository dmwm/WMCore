"""
_UpdateProgress_

Oracle implementation of WorkQueueElement.UpdateProgress
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.UpdateReqMgr \
     import UpdateReqMgr as UpdateReqMgrMySQL

class UpdateReqMgr(UpdateReqMgrMySQL):
    pass
