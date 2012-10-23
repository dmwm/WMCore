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
    sql = """SELECT rj.wmbs_id jobid, rj.grid_id gridid, rj.bulk_id bulkid,
               st.name status, rj.retry_count retry_count, rj.id id,
               rj.status_time status_time, wu.cert_dn AS userdn,
               wu.group_name AS usergroup, wu.role_name AS userrole,
               wj.cache_dir AS cache_dir
             FROM bl_runjob rj
             INNER JOIN bl_status st ON rj.sched_status = st.id
             LEFT OUTER JOIN wmbs_users wu ON wu.id = rj.user_id
             INNER JOIN wmbs_job wj ON wj.id = rj.wmbs_id
             WHERE rj.status = 1
             """

    def execute(self, conn = None, transaction = False):
        """
        _execute_

        """
        result = self.dbi.processData(self.sql, binds = {}, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
