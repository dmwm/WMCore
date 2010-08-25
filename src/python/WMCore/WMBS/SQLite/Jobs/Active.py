#!/usr/bin/env python
"""
_Active_

SQLite implementation of Jobs.Active
"""

from WMCore.WMBS.MySQL.Jobs.Active import Active as ActiveJobsMySQL

class Active(ActiveJobsMySQL):
    insertSQL = """INSERT INTO wmbs_group_job_acquired (job, jobgroup)
                     SELECT :job, (SELECT jobgroup FROM wmbs_job WHERE id = :job)
                       WHERE NOT EXISTS
                         (SELECT job FROM wmbs_group_job_acquired WHERE job = :job)"""        
