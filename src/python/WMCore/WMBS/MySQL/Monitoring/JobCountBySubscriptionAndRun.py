#!/usr/bin/env python
"""
_JobCountBySubscription_

Monitoring DAO classes for Jobs in WMBS
"""
__all__ = []



from WMCore.WMBS.MySQL.Monitoring.DefaultFormatter import DefaultFormatter

class JobCountBySubscriptionAndRun(DefaultFormatter):
    """
    _JobCountBySubscription_

    return the number of jobs grouped by their status and run for given a subscription (fileset, workflow pair)
    """
    # warning oracle keyword is written upper case for easy replacement - not the best way to handle this
    sql = """SELECT wmbs_file_runlumi_map.run, wmbs_job_state.name AS job_state, count(DISTINCT wmbs_job.id) AS numJobs
             FROM wmbs_job
              INNER JOIN wmbs_job_state ON wmbs_job_state.id = wmbs_job.state
              INNER JOIN wmbs_job_assoc ON wmbs_job_assoc.job = wmbs_job.id
              INNER JOIN wmbs_file_runlumi_map ON wmbs_file_runlumi_map.fileid = wmbs_job_assoc.fileid
              INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
              INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
              INNER JOIN wmbs_fileset ON wmbs_fileset.id = wmbs_subscription.fileset
              INNER JOIN wmbs_workflow ON wmbs_workflow.id = wmbs_subscription.workflow
             WHERE wmbs_fileset.name = :fileset_name AND wmbs_workflow.name = :workflow_name
                   AND wmbs_file_runlumi_map.run = :run
             GROUP BY wmbs_job_state.name, wmbs_file_runlumi_map.run"""

    def execute(self, fileset_name, workflow_name, run, conn = None, transaction = False):
        """
        _execute_

        Execute the SQL for the given job ID and then format and return
        the result.
        """
        bindVars = {"fileset_name": fileset_name, "workflow_name": workflow_name, "run": run}

        result = self.dbi.processData(self.sql, bindVars, conn = conn,
                                      transaction = transaction)

        return self.formatDict(result)
