"""
_WorkUnit.GetStatesByJob_

MySQL implementation of WorkUnit.GetStatesByJob
"""

from __future__ import absolute_import, division, print_function

from WMCore.Database.DBFormatter import DBFormatter


class GetStatesByJob(DBFormatter):
    """
    _GetState_

    Given a job, get the state(s) of the workunits attached to that job
    """

    sql = ('SELECT id, status, retry_count FROM wmbs_workunit '
           'INNER JOIN wmbs_job_workunit_assoc ON wmbs_workunit.id = wmbs_job_workunit_assoc.workunit '
           'WHERE wmbs_job_workunit_assoc.job = :job'
           )

    def format(self, results):
        """
        _formatDict_

        Return a name from MySQL
        """

        wuResults = []

        if len(results) == 0:
            return False
        else:
            for row in results[0].fetchall():
                wuid, status, retries = row
                wuResults.append({'wuid': wuid, 'status': status, 'retries': retries})
            return wuResults

    def execute(self, job, conn=None, transaction=False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """

        result = self.dbi.processData(self.sql, {'job': job['id']}, conn=conn, transaction=transaction)

        return self.format(result)
