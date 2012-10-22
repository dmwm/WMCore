#!/usr/bin/env python
"""
_LoadJobs_

Oracle implementation of JobGroup.LoadJobs
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.LoadJobs import LoadJobs as LoadJobsJobGroupMySQL

class LoadJobs(LoadJobsJobGroupMySQL):
    sql = LoadJobsJobGroupMySQL.sql
