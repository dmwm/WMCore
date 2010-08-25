"""
_WorkloadsByName_

Oracle implementation of Monitor..Workloads.WorkloadsByName
"""

__all__ = []
__revision__ = "$Id: WorkloadsByName.py,v 1.1 2010/06/03 17:07:19 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Workloads.WorkloadsByName \
     import WorkloadsByName as WorkloadsByNameMySQL

class WorkloadsByName(WorkloadsByNameMySQL):
    pass
