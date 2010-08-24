"""
_WorkloadsByName_

Oracle implementation of Monitor..Workloads.WorkloadsByName
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Workloads.WorkloadsByName \
     import WorkloadsByName as WorkloadsByNameMySQL

class WorkloadsByName(WorkloadsByNameMySQL):
    pass
