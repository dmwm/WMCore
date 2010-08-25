"""
_WorkloadsByOwner_

Oracle implementation of Monitor..Workloads.WorkloadsByOwner
"""

__all__ = []
__revision__ = "$Id: WorkloadsByOwner.py,v 1.1 2010/06/03 17:07:19 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Workloads.WorkloadsByOwner \
     import WorkloadsByOwner as WorkloadsByOwnerMySQL

class WorkloadsByOwner(WorkloadsByOwnerMySQL):
    pass
