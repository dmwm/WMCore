#!/usr/bin/env python
"""
_NewJobs_

Oracle implementation for creating a new job
"""


from WMCore.BossAir.MySQL.NewJobs import NewJobs as MySQLNewJobs

class NewJobs(MySQLNewJobs):
    """
    _NewJobs_

    Insert new jobs into bl_runjob
    """


    sql = """INSERT INTO bl_runjob (id, wmbs_id, grid_id, bulk_id, sched_status, retry_count, user)
               VALUES (bl_runjob_SEQ.nextval, :jobid, :gridid, :bulkid,
                 (SELECT id FROM bl_status WHERE name = :status),
                 :retry_count,
                 (SELECT id FROM wmbs_users WHERE cert_dn = :user)
             )"""

