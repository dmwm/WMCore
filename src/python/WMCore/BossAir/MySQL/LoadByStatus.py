#!/usr/bin/env python
"""
_LoadByStatus_

MySQL implementation for loading a job by scheduler status
"""


from WMCore.Database.DBFormatter import DBFormatter

class LoadByStatus(DBFormatter):
    """
    _LoadByStatus_

    Load all jobs with a certain scheduler status
    """


    sql = """SELECT rj.wmbs_id AS jobid, rj.grid_id AS gridid, rj.bulk_id AS bulkid,
               st.name AS status, rj.retry_count as retry_count, rj.id AS id, wu.cert_dn AS userdn,
               wu.group_name AS usergroup, wu.role_name AS userrole
               FROM bl_runjob rj
               INNER JOIN bl_status st ON rj.sched_status = st.id
               LEFT OUTER JOIN wmbs_users wu ON wu.id = rj.user_id
               WHERE rj.status = :complete AND st.name = :status
    """



    def execute(self, status, complete = '1', conn = None, transaction = False):
        """
        _execute_

        Create new jobs
        Expect all values in the binds field
        """

        binds = {'complete': complete, 'status': status}

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
