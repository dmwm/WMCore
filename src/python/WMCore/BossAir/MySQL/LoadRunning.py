#!/usr/bin/env python
"""
_LoadRunning_

MySQL implementation for loading a job by scheduler status
"""


from WMCore.Database.DBFormatter import DBFormatter

class LoadRunning(DBFormatter):
    """
    _LoadRunning_

    Load all jobs with a certain scheduler status
    """


    sql = """SELECT rj.wmbs_id AS jobid, rj.grid_id AS gridid, rj.bulk_id AS bulkid,
               st.name AS status, rj.retry_count as retry_count, rj.id AS id,
               rj.status_time as status_time
               FROM bl_runjob rj
               INNER JOIN bl_status st ON rj.sched_status = st.id
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
