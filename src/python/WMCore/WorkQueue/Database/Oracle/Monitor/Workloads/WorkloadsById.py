"""
_WorkloadsById_

Oracle implementation of Monitor..Workloads.WorkloadsById
"""

__all__ = []
__revision__ = "$Id: WorkloadsById.py,v 1.1 2010/06/03 17:07:19 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Workloads.WorkloadsById \
     import WorkloadsById as WorkloadsByIdMySQL

class WorkloadsById(WorkloadsByIdMySQL):
    pass
