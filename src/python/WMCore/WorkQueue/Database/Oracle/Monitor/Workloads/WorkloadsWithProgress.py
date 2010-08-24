"""
_WorkloadsWithProgress_

Oracle implementation of Monitor..Workloads.WorkloadsWithProgress
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Workloads.WorkloadsWithProgress \
     import WorkloadsWithProgress as WorkloadsWithProgressMySQL

class WorkloadsWithProgress(WorkloadsWithProgressMySQL):
    pass
