#!/usr/bin/env python
"""
_NewJobs_

MySQL implementation for creating a new job
"""

import time

from WMCore.Database.DBFormatter import DBFormatter


class NewJobs(DBFormatter):
    """
    _NewJobs_

    Insert new jobs into bl_runjob
    """


    sql = """INSERT IGNORE INTO bl_runjob (wmbs_id, grid_id, bulk_id, sched_status,
                 retry_count, user_id, location, status_time)
               VALUES (:jobid, :gridid, :bulkid,
                 (SELECT id FROM bl_status WHERE name = :status),
                 :retry_count,
                 (SELECT id FROM wmbs_users WHERE cert_dn = :userdn AND group_name = :usergroup AND role_name = :userrole),
                 (SELECT id FROM wmbs_location WHERE site_name = :location),
                 :status_time
          )"""



    def execute(self, jobs, conn = None, transaction = False):
        """
        _execute_

        Create new jobs
        Expect all values in the binds field
        """

        if len(jobs) == 0:
            return

        statusTime = int(time.time())
        binds = []
        for job in jobs:
            binds.append({'jobid': job['jobid'], 'gridid': job.get('gridid', None), 'bulkid': job.get('bulkid', None),
                          'status': job.get('status', None), 'retry_count': job['retry_count'], 'userdn': job['userdn'],
                          'usergroup': job['usergroup'], 'userrole': job['userrole'],
                          'location': job.get('siteName'), 'status_time': statusTime})

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return
