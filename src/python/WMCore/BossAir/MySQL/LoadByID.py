#!/usr/bin/env python
"""
_LoadByID_

MySQL implementation for loading a job by scheduler status
"""


from WMCore.Database.DBFormatter import DBFormatter

class LoadByID(DBFormatter):
    """
    _LoadByID_

    Load all jobs in full by ID
    """


    sql = """SELECT rj.wmbs_id AS jobid, rj.grid_id AS gridid, rj.bulk_id AS bulkid,
               st.name AS status, rj.retry_count AS retry_count, rj.id AS id,
               rj.status_time AS status_time, wl.plugin AS plugin, wu.cert_dn AS userdn,
               wj.cache_dir AS cache_dir
               FROM bl_runjob rj
               LEFT OUTER JOIN wmbs_users wu ON wu.id = rj.user_id
               INNER JOIN bl_status st ON rj.sched_status = st.id
               INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
               LEFT OUTER JOIN wmbs_location wl ON wl.id = wj.location
               WHERE rj.id = :id
    """



    def execute(self, jobs, conn = None, transaction = False):
        """
        _execute_

        Load jobs in full via ID
        """

        binds = []
        for job in jobs:
            binds.append({'id': job['id']})


        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
