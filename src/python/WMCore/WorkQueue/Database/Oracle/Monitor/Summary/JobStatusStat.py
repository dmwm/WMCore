"""
_JobStatusStat_

Oracle implementation of Monitor.Summary.JobStatusStat
"""

__all__ = []
__revision__ = "$Id: JobStatusStat.py,v 1.1 2010/06/03 17:07:20 sryu Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WorkQueue.Database.MySQL.Monitor.Summary.JobStatusStat \
     import JobStatusStat as JobStatusStatMySQL

class JobStatusStat(JobStatusStatMySQL):
    pass