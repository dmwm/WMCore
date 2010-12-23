"""
_TopLevelJobsByRequest_

SQLite implementation of Monitor.Summary.TopLevelJobsByRequest
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.TopLevelJobsByRequest \
     import TopLevelJobsByRequest as TopLevelJobsByRequestMySQL

class TopLevelJobsByRequest(TopLevelJobsByRequestMySQL):
    pass