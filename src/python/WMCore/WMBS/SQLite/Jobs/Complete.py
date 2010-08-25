#!/usr/bin/env python
"""
_Complete_

SQLite implementation of Jobs.Complete
"""

from WMCore.WMBS.MySQL.Jobs.Complete import Complete as CompleteJobsMySQL

class Complete(CompleteJobsMySQL):
    insertSQL = """INSERT INTO wmbs_group_job_complete (job, jobgroup)
                     SELECT :job, (SELECT jobgroup FROM wmbs_job WHERE id = :job)
                       WHERE NOT EXISTS
                         (SELECT job FROM wmbs_group_job_complete WHERE job = :job)"""    
