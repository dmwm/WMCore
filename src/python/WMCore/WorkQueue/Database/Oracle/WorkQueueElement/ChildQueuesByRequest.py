"""
_ChildQueuesByRequest_

Oracle implementation of WorkQueueElement.GhildQueuesByRequest
"""
__all__ = []



from WMCore.WorkQueue.Database.MySQL.WorkQueueElement.ChildQueuesByRequest \
     import ChildQueuesByRequest as ChildQueuesByRequestMySQL

class ChildQueuesByRequest(ChildQueuesByRequestMySQL):
    pass