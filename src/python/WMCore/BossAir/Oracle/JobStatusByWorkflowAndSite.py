#!/usr/bin/env python
"""
_JobStatusByWorkflowAndSite_

Oracle implementation for loading a job by scheduler status
"""


from WMCore.BossAir.MySQL.JobStatusByWorkflowAndSite \
     import JobStatusByWorkflowAndSite as MySQLJobStatusByWorkflowAndSite

class JobStatusByWorkflowAndSite(MySQLJobStatusByWorkflowAndSite):
    pass
