"""
_RunJobByStatus_

Monitoring DAO classes for Jobs in BossAir database
"""
from __future__ import print_function, division

from WMCore.Database.DBFormatter import DBFormatter


class RunJobByStatus(DBFormatter):
    sql = """
          SELECT bl_status.name AS sched_status, count(*) AS count
            FROM bl_runjob
            INNER JOIN bl_status ON bl_runjob.sched_status = bl_status.id
            LEFT OUTER JOIN wmbs_users ON wmbs_users.id = bl_runjob.user_id
            INNER JOIN wmbs_job ON wmbs_job.id = bl_runjob.wmbs_id
            WHERE bl_runjob.status = :status GROUP BY bl_status.name
          """

    def formatDict(self, results):
        """
        _formatDict_

        Format the results in a dict keyed by the state name
        """
        formattedResults = DBFormatter.formatDict(self, results)

        dictResult = {}
        for formattedResult in formattedResults:
            dictResult[formattedResult['sched_status']] = int(formattedResult['count'])

        return dictResult

    def execute(self, active=True, conn=None, transaction=False):
        if active:
            binds = {'status': 1}
        else:
            binds = {'status': 0}

        result = self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)

        return self.formatDict(result)
