#!/usr/bin/env python
"""
_LoadJobs_

SQLite implementation of JobGroup.LoadJobs
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.LoadJobs import LoadJobs as LoadJobsMySQL

class LoadJobs(LoadJobsMySQL):
    sql = LoadJobsMySQL.sql
