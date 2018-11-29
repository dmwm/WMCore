#!/usr/bin/env python
"""
_JobsByState_

Monitoring DAO classes for Jobs in WMBS
"""

from WMCore.Database.DBFormatter import DBFormatter


class JobCountByState(DBFormatter):
    sql = """SELECT wmbs_job_state.name AS job_state, count(wmbs_job.state) AS job_count
               FROM wmbs_job_state
               LEFT OUTER JOIN wmbs_job ON wmbs_job.state=wmbs_job_state.id
             GROUP BY wmbs_job_state.name"""

    def formatDict(self, results):
        """
        _formatDict_

        Format the results in a dict keyed by the state name
        """
        formattedResults = DBFormatter.formatDict(self, results)

        dictResult = {}
        for formattedResult in formattedResults:
            dictResult[formattedResult['job_state']] = int(formattedResult['job_count'])

        return dictResult

    def execute(self, conn=None, transaction=False, returnCursor=False):
        """
        _execute_

        Execute the SQL.
        """
        result = self.dbi.processData(self.sql, conn=conn, transaction=transaction)

        return self.formatDict(result)
