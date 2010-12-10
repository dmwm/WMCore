"""
_JobStatusByRequest_

SQLite implementation of Monitor.Summary.JobStatusByRequest
"""

__all__ = []



from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.JobStatusByRequest \
     import JobStatusByRequest as JobStatusByRequestMySQL

class JobStatusByRequest(JobStatusByRequestMySQL):
    pass