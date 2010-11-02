"""
_StatusStatistics_

SQLite implementation of Monitor.Summary.StatusStatistics
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.StatusStatistics \
     import StatusStatistics as StatusStatisticsMySQL

class StatusStatistics(StatusStatisticsMySQL):
    pass
