"""
_RunJobByStatus_

Monitoring DAO classes for Jobs in BossAir database.
It groups jobs in each sched_status and bossAir status and guarantee
all sched_status are always present in the output.
"""
from __future__ import print_function, division

from WMCore.Database.DBFormatter import DBFormatter


class RunJobByStatus(DBFormatter):
    sql = """
          SELECT bl_status.name AS sched_status, count(bl_runjob.sched_status) AS count, bl_runjob.status
            FROM bl_status
            LEFT OUTER JOIN bl_runjob ON bl_runjob.sched_status = bl_status.id
            GROUP BY bl_status.name, bl_runjob.status
          """

    def formatDict(self, results):
        """
        _formatDict_

        Creates a dictionary of active (status=1) and completed (status=0)
        jobs in BossAir with their sched_status and the amount of jobs in that status
        """
        formattedResults = DBFormatter.formatDict(self, results)

        results = {'active': {}, 'completed': {}}
        for res in formattedResults:
            results['active'].setdefault(res['sched_status'], 0)
            results['completed'].setdefault(res['sched_status'], 0)
            if res['status'] is None:
                pass  # job count is always 0 for this case
            elif int(res['status']) == 0:
                results['completed'][res['sched_status']] += int(res['count'])
            else:  # status = 1
                results['active'][res['sched_status']] += int(res['count'])

        return results

    def execute(self, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, conn=conn, transaction=transaction)

        return self.formatDict(result)
