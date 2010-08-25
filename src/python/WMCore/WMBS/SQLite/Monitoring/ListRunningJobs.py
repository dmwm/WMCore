#!/usr/bin/env python
"""
_ListRunningJobs_

SQLite implementation of Monitoring.ListRunningJobs
"""




from WMCore.WMBS.MySQL.Monitoring.ListRunningJobs import ListRunningJobs \
    as ListRunningJobsMySQL

class ListRunningJobs(ListRunningJobsMySQL):
    pass
