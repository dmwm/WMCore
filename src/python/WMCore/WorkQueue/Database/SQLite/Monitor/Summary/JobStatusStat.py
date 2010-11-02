"""
_JobStatusStat_

SQLite implementation of Monitor.Summary.JobStatusStat
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.JobStatusStat \
     import JobStatusStat as JobStatusStatMySQL

class JobStatusStat(JobStatusStatMySQL):
    pass