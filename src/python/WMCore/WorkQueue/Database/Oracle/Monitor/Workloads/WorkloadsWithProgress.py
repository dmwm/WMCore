"""
_WorkloadsWithProgress_

Oracle implementation of Monitor..Workloads.WorkloadsWithProgress
"""

__all__ = []
__revision__ = "$Id: WorkloadsWithProgress.py,v 1.1 2010/06/03 17:07:19 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Workloads.WorkloadsWithProgress \
     import WorkloadsWithProgress as WorkloadsWithProgressMySQL

class WorkloadsWithProgress(WorkloadsWithProgressMySQL):
    pass
