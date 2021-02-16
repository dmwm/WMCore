#!/usr/bin/env python
"""
_GetLocation_

MySQL implementation of Jobs.GetAllJobs
"""

from WMCore.Database.DBFormatter import DBFormatter

from future.utils import listvalues

class GetAllJobs(DBFormatter):
    """
    _GetLocation_

    Retrieve all files that are associated with the given job from the
    database.
    """
    sql_all = "SELECT id FROM wmbs_job"

    sql_state = "SELECT id FROM wmbs_job WHERE state = (SELECT id FROM wmbs_job_state WHERE name = :state)"

    sql_state_type = """SELECT wmbs_job.id FROM wmbs_job
                          INNER JOIN wmbs_jobgroup ON wmbs_job.jobgroup = wmbs_jobgroup.id
                          INNER JOIN wmbs_subscription ON wmbs_jobgroup.subscription = wmbs_subscription.id
                          INNER JOIN wmbs_job_state ON wmbs_job.state = wmbs_job_state.id
                          INNER JOIN wmbs_sub_types ON wmbs_subscription.subtype = wmbs_sub_types.id
                          WHERE wmbs_job_state.name = :state
                          AND wmbs_sub_types.name = :type"""

    limit_sql = " limit %d"

    def format(self, results):
        """
        _formatDict_

        Cast the file attribute to an integer, and also handle changing the
        column name in Oracle from FILEID to FILE.
        """

        if len(results) == 0:
            return False
        else:
            tempList = results[0].fetchall()
            final = []
            for i in tempList:
                final.append(listvalues(i)[0])
            return final

    def execute(self, state=None, jobType=None, conn=None,
                transaction=False, limitRows=None):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        if limitRows:
            extraSql = self.limit_sql % limitRows
        else:
            extraSql = ""

        if state is None:
            result = self.dbi.processData(self.sql_all + extraSql, {}, conn=conn,
                                          transaction=transaction)
        else:
            if jobType:
                result = self.dbi.processData(self.sql_state_type + extraSql, {'state': state.lower(), 'type': jobType},
                                              conn=conn, transaction=transaction)
            else:
                result = self.dbi.processData(self.sql_state + extraSql, {'state': state.lower()},
                                              conn=conn, transaction=transaction)

        res = self.format(result)
        return res
