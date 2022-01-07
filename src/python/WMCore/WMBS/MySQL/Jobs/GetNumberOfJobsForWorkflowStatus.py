#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MySQL implementation for retrieving the total number of jobs
for a given workflow in a given wmbs status
"""
from __future__ import print_function, division

from WMCore.Database.DBFormatter import DBFormatter


class GetNumberOfJobsForWorkflowStatus(DBFormatter):
    """
    Get the number of jobs for a workflow/task combination
    in a given job status.
    """

    sql = """SELECT COUNT(*) FROM wmbs_job
             INNER JOIN wmbs_jobgroup ON wmbs_job.jobgroup = wmbs_jobgroup.id
             INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
             INNER JOIN wmbs_workflow ON wmbs_subscription.workflow = wmbs_workflow.id
             WHERE wmbs_workflow.name = :workflow AND
                   wmbs_job.state = (SELECT id FROM wmbs_job_state WHERE name = :status)
               """

    def execute(self, workflow, status='executing', conn=None, transaction=False):
        """
        Execute the SQL and return the number of jobs

        :param workflow: string with the workflow name
        :param status: string with the wmbs state
        :param conn: database connection object
        :param transaction: boolean to use or not a transaction
        :return: integer with the total number of jobs
        """
        binds = {'workflow': workflow, 'status': status}
        result = self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)

        return self.format(result)[0][0]
