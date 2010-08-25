"""
_WorkloadsByOwner_

Oracle implementation of Monitor..Workloads.WorkloadsByOwner
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Workloads.WorkloadsByOwner \
     import WorkloadsByOwner as WorkloadsByOwnerMySQL

class WorkloadsByOwner(WorkloadsByOwnerMySQL):
    pass
