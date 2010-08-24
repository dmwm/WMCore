#!/usr/bin/env python
"""
_Active_
SQLite implementation of Jobs.Active

move file into wmbs_group_job_acquired
"""

from WMCore.WMBS.MySQL.Jobs.Active import Active as ActiveJobsMySQL

class Active(ActiveJobsMySQL):
    sql = ActiveJobsMySQL.sql
