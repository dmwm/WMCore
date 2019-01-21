#!/usr/bin/env python
"""
_UpdateJobs_

MySQL implementation for updating jobs
"""

from WMCore.Database.DBFormatter import DBFormatter


class UpdateJobs(DBFormatter):
    """
    _UpdateJobs_

    Update jobs with new values
    """

    sql = """UPDATE bl_runjob SET wmbs_id = :jobid, grid_id = :gridid,
               bulk_id = :bulkid, status_time = :status_time,
               sched_status = (SELECT id FROM bl_status WHERE name = :status),
               retry_count = :retry_count,
               user_id = (SELECT id FROM wmbs_users WHERE cert_dn = :owner AND group_name = :usergroup AND role_name = :userrole)
               WHERE id = :id
               """

    def execute(self, jobs, conn=None, transaction=False):
        """
        _execute_

        Update jobs with new values.
        Mostly a maintenance script
        """

        if len(jobs) == 0:
            return

        binds = []
        for job in jobs:
            binds.append({'jobid': job['jobid'], 'gridid': job.get('gridid', None), 'bulkid': job.get('bulkid', None),
                          'status': job.get('status', None), 'retry_count': job['retry_count'], 'id': job['id'],
                          'status_time': job.get('status_time', None), 'owner': job['userdn'],
                          'usergroup': job['usergroup'], 'userrole': job['userrole']})

        self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)

        return
