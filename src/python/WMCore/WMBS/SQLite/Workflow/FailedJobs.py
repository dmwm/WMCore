#!/usr/bin/env python
"""
_FailedJobs_

SQLite implementation of Workflow.FailedJobs
"""




from WMCore.WMBS.MySQL.Workflow.FailedJobs import FailedJobs as MySQLFailedJobs

class FailedJobs(MySQLFailedJobs):
    pass
    
