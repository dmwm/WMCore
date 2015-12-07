"""
_GetNumberOfJobsForWorkflowTaskStatus_

MySQL implementation of GetNumberOfJobsForWorkflowTaskStatus

Created on Apr 17, 2013

@author: dballest
"""
__all__ = []

from WMCore.Database.DBFormatter import DBFormatter

class GetNumberOfJobsForWorkflowTaskStatus(DBFormatter):
    """
    Get the number of jobs for a workflow/task combination
    in a given job status.
    """

    sql = """SELECT COUNT(*) FROM wmbs_job
             INNER JOIN wmbs_jobgroup ON wmbs_job.jobgroup = wmbs_jobgroup.id
             INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
             INNER JOIN wmbs_workflow ON wmbs_subscription.workflow = wmbs_workflow.id
             WHERE wmbs_workflow.name = :workflow AND
                   wmbs_workflow.task = :task AND
                   wmbs_job.state = (SELECT id FROM wmbs_job_state WHERE name = :status)
               """

    def execute(self, workflow, task, status = 'executing', conn = None, transaction = False):
        """
        _execute_

        Execute the SQL and return the number of jobs
        """

        binds = {'workflow': workflow, 'task' : task, 'status' : status}

        result = self.dbi.processData(self.sql, binds,
                                      conn = conn, transaction = transaction)

        return self.format(result)[0][0]
