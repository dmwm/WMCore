#!/usr/bin/env python
"""
_LoadByWMBSID_

MySQL implementation for loading a job by WMBS info
"""

from WMCore.Database.DBFormatter import DBFormatter


class LoadByWMBSID(DBFormatter):
    """
    _LoadByWMBSID_

    Load all jobs in full by WMBS ID and retry count
    """

    sql = """SELECT rj.wmbs_id AS jobid, rj.grid_id AS gridid, rj.bulk_id AS bulkid,
               st.name AS status, rj.retry_count as retry_count, rj.id AS id,
               rj.status_time as status_time, wu.cert_dn AS userdn,
               wu.group_name AS usergroup, wu.role_name AS userrole,
               wl.plugin AS plugin, wj.cache_dir AS cache_dir
               FROM bl_runjob rj
               INNER JOIN bl_status st ON rj.sched_status = st.id
               LEFT OUTER JOIN wmbs_users wu ON wu.id = rj.user_id
               INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
               INNER JOIN wmbs_location wl ON wl.id = wj.location
               WHERE rj.wmbs_id = :id AND rj.retry_count = :retry_count"""

    def execute(self, jobs, conn=None, transaction=False):
        """
        _execute_

        Load jobs in full via WMBS ID and retry count
        Only useful when killing outside of tracker/submitter loop
        where you don't have any bossAir info
        """

        if len(jobs) < 1:
            # No jobs to run
            return

        binds = []
        for job in jobs:
            binds.append({'id': job['id'], 'retry_count': job['retry_count']})

        result = self.dbi.processData(self.sql, binds, conn=conn,
                                      transaction=transaction)

        return self.formatDict(result)
