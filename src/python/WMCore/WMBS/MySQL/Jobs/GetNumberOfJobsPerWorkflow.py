#!/usr/bin/env python
"""
_GetNumberOfJobsPerWorkflow_

MySQL implementation of Jobs.GetNumberOfJobsPerWorkflow
"""

__all__ = []



import logging

from WMCore.Database.DBFormatter import DBFormatter

class GetNumberOfJobsPerWorkflow(DBFormatter):
    """
    Exactly what it says on the tin

    """

    sql = """SELECT COUNT(*) FROM wmbs_job
               INNER JOIN wmbs_jobgroup ON wmbs_job.jobgroup = wmbs_jobgroup.id
               INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
               WHERE wmbs_subscription.workflow = :workflow
               """


    def execute(self, workflow, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """

        binds = {'workflow': workflow}

        result = self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)

        return self.format(result)[0][0]  #Should only be one result
