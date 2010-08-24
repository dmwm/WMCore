"""
_StatusStatByWorkload_

Oracle implementation of Monitor.Summary.StatusStatByWorkload
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.StatusStatByWorkload \
     import StatusStatByWorkload as StatusStatByWorkloadMySQL

class StatusStatByWorkload(StatusStatByWorkloadMySQL):
    pass