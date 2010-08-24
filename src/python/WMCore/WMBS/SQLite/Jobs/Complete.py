#!/usr/bin/env python
"""
_Complete_
SQLite implementation of Jobs.Complete

move file into wmbs_group_job_completed
"""

from WMCore.WMBS.MySQL.Jobs.Complete import Complete as CompleteJobsMySQL

class Complete(CompleteJobsMySQL):
    sql = CompleteJobsMySQL.sql
