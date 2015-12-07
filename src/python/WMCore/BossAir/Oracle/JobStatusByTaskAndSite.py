#!/usr/bin/env python
"""
_JobStatusByTaskAndSite_

Oracle implementation for loading a job by scheduler status
"""


from WMCore.BossAir.MySQL.JobStatusByTaskAndSite \
     import JobStatusByTaskAndSite as MySQLJobStatusByTaskAndSite

class JobStatusByTaskAndSite(MySQLJobStatusByTaskAndSite):
    pass
