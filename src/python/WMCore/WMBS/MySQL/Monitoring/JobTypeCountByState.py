"""
_JobTypeCountByState_

Monitoring DAO classes for Jobs in WMBS
"""
from __future__ import print_function, division

from WMCore.Database.DBFormatter import DBFormatter


class JobTypeCountByState(DBFormatter):
    sql = """
          SELECT wmbs_sub_types.name AS job_type, COUNT(*) AS count FROM wmbs_job
            INNER JOIN wmbs_jobgroup ON wmbs_job.jobgroup = wmbs_jobgroup.id
            INNER JOIN wmbs_subscription ON wmbs_jobgroup.subscription = wmbs_subscription.id
            INNER JOIN wmbs_sub_types ON wmbs_subscription.subtype = wmbs_sub_types.id
            WHERE wmbs_job.state = (SELECT id FROM wmbs_job_state WHERE name = :state)
            GROUP BY wmbs_subscription.subtype, wmbs_sub_types.name
          """

    def formatDict(self, results):
        """
        _formatDict_

        Format the results in a dict keyed by the state name
        """
        formattedResults = DBFormatter.formatDict(self, results)

        dictResult = {}
        for formattedResult in formattedResults:
            dictResult[formattedResult['job_type']] = int(formattedResult['count'])

        return dictResult

    def execute(self, state, conn=None, transaction=False):
        binds = {'state': state}
        result = self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)

        return self.formatDict(result)
