#!/usr/bin/env python
"""
_JobStatusForMonitoring_

Oracle implementation for loading a job by scheduler status
"""


from WMCore.BossAir.MySQL.JobStatusForMonitoring \
     import JobStatusForMonitoring as MySQLJobStatusForMonitoring

class JobStatusForMonitoring(MySQLJobStatusForMonitoring):
    pass