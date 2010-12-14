#!/usr/bin/env python
"""
_LoadRunning_

MySQL implementation for loading bl_runjob records for active jobs
"""


from WMCore.Database.DBFormatter import DBFormatter

class LoadRunning(DBFormatter):
    """
    _LoadRunning_

    Load all bl_runjob that are active

    """
    sql = """SELECT rj.wmbs_id AS jobid, rj.grid_id AS gridid, rj.bulk_id AS bulkid, 
               st.name AS status, rj.retry_count AS retry_count, rj.id AS id, 
               rj.status_time AS status_time, wu.cert_dn AS user
             FROM bl_runjob rj 
             INNER JOIN bl_status st ON rj.sched_status = st.id
             INNER JOIN wmbs_users wu ON wu.id = rj.user_id
             WHERE rj.status = 1
             """

    def execute(self, conn = None, transaction = False):
        """
        _execute_

        """
        result = self.dbi.processData(self.sql, binds = {}, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
