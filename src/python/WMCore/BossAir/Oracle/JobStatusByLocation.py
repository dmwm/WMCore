#!/usr/bin/env python
"""
_JobStatusByLocation_

Oracle implementation for loading a job by scheduler status
"""


from WMCore.BossAir.MySQL.JobStatusByLocation \
     import JobStatusByLocation as MySQLJobStatusByLocation

class JobStatusByLocation(MySQLJobStatusByLocation):
    pass
