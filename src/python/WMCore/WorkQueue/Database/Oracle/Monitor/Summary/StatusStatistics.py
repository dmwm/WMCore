"""
_StatusStatistics_

Oracle implementation of Monitor.Summary.StatusStatistics
"""

__all__ = []
__revision__ = "$Id: StatusStatistics.py,v 1.1 2010/06/03 17:07:20 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.StatusStatistics \
     import StatusStatistics as StatusStatisticsMySQL

class StatusStatistics(StatusStatisticsMySQL):
    pass
