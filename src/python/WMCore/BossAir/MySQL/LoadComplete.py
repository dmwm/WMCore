#!/usr/bin/env python
"""
_LoadComplete_

MySQL implementation for loading bl_runjob records for complete jobs
"""
import logging

from WMCore.Database.DBFormatter import DBFormatter

class LoadComplete(DBFormatter):
    """
    _LoadComplete_

    Load all bl_runjob that are complete, where the
    corresponding wmbs_job entry is still in 'executing'
    and where the retry_count match between bl_runjob
    and wmbs_job. The later makes sure we only look at
    the latest bl_runjob instance for a given wmbs_job.

    The two query implementations give identical results,
    in a congested database (few wmbs_job in executing
    and almost all bl_runjob with status 0) the second
    one is likely faster (not tested).

    """

    sql = """SELECT bl_runjob.wmbs_id AS jobid,
                    bl_runjob.grid_id AS gridid,
                    bl_runjob.bulk_id AS bulkid,
                    bl_status.name AS status,
                    bl_runjob.retry_count AS retry_count,
                    bl_runjob.id AS id,
                    bl_runjob.status_time AS status_time,
                    wmbs_job.name AS name,
                    wmbs_location.ce_name AS location,
                    wmbs_users.cert_dn AS userdn,
                    wmbs_users.group_name AS usergroup, wmbs_users.role_name AS userrole
             FROM wmbs_job
             INNER JOIN bl_runjob ON
               bl_runjob.wmbs_id = wmbs_job.id AND
               bl_runjob.retry_count = wmbs_job.retry_count AND
               bl_runjob.status = 0
             INNER JOIN bl_status ON
               bl_status.id = bl_runjob.sched_status
             LEFT OUTER JOIN wmbs_users ON wmbs_users.id = bl_runjob.user_id
             INNER JOIN wmbs_location ON wmbs_location.id = bl_runjob.location
             WHERE wmbs_job.state = (SELECT id FROM wmbs_job_state WHERE name = 'executing')
             """

    def execute(self, conn = None, transaction = False):
        """
        _execute_

        """
        result = self.dbi.processData(self.sql, binds = {}, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
