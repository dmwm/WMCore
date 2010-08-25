"""
_StatusStatByWorkload_

Oracle implementation of Monitor.Summary.StatusStatByWorkload
"""

__all__ = []
__revision__ = "$Id: StatusStatByWorkload.py,v 1.1 2010/06/03 17:07:20 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.StatusStatByWorkload \
     import StatusStatByWorkload as StatusStatByWorkloadMySQL

class StatusStatByWorkload(StatusStatByWorkloadMySQL):
    pass