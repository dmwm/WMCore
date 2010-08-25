#!/usr/bin/env python
"""
_Failed_

SQLite implementation of Jobs.Failed
"""

from WMCore.WMBS.MySQL.Jobs.Failed import Failed as FailedJobsMySQL

class Failed(FailedJobsMySQL):
    insertSQL = """INSERT INTO wmbs_group_job_failed (job, jobgroup)
                     SELECT :job, (SELECT jobgroup FROM wmbs_job WHERE id = :job)
                       WHERE NOT EXISTS
                         (SELECT job FROM wmbs_group_job_failed WHERE job = :job)"""
