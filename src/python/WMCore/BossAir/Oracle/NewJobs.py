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

    sql = """INSERT INTO bl_runjob (id, wmbs_id, grid_id, bulk_id, sched_status,
                 retry_count, user_id, location, status_time)
               SELECT bl_runjob_SEQ.nextval, :jobid, :gridid, :bulkid,
                 (SELECT id FROM bl_status WHERE name = :status),
                 :retry_count,
                 (SELECT id FROM wmbs_users WHERE cert_dn = :userdn AND group_name = :usergroup AND role_name = :userrole),
                 (SELECT id FROM wmbs_location WHERE site_name = :location),
                 :status_time
               FROM dual
               WHERE NOT EXISTS (SELECT id FROM bl_runjob WHERE wmbs_id = :jobid
                                   AND retry_count = :retry_count)"""
