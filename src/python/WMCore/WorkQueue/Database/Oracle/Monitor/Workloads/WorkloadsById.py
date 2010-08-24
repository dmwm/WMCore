"""
_WorkloadsById_

Oracle implementation of Monitor..Workloads.WorkloadsById
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Workloads.WorkloadsById \
     import WorkloadsById as WorkloadsByIdMySQL

class WorkloadsById(WorkloadsByIdMySQL):
    pass
