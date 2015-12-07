#!/usr/bin/env python
"""
_LoadForMonitoring_

MySQL implementation for loading a job by scheduler status
"""


from WMCore.Database.DBFormatter import DBFormatter

class LoadForMonitoring(DBFormatter):
    """
    _LoadForMonitoring_

    Load all jobs with a certain scheduler status including
    all the joined information.
    """


    sql = """SELECT rj.wmbs_id AS jobid, rj.grid_id AS gridid, rj.bulk_id AS bulkid,
               st.name AS status, rj.retry_count as retry_count, rj.id AS id,
               rj.status_time as status_time, wl.plugin AS plugin, wu.cert_dn AS owner
               FROM bl_runjob rj
               INNER JOIN bl_status st ON rj.sched_status = st.id
               LEFT OUTER JOIN wmbs_users wu ON wu.id = rj.user_id
               INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
               LEFT OUTER JOIN wmbs_location wl ON wl.id = wj.location
               WHERE rj.status = :complete
    """



    def execute(self, complete = '1', conn = None, transaction = False):
        """
        _execute_

        Load all jobs either running or not (running by default)
        """

        binds = {'complete': complete}


        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
