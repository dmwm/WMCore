#!/usr/bin/env python
"""
_KillWorkflow_

MySQL implementation of Jobs.KillWorkflow
"""

from WMCore.Database.DBFormatter import DBFormatter

class KillWorkflow(DBFormatter):
    """
    _KillWorkflow_

    Find all jobs that belong don't belong to Cleanup and LogCollect
    subscriptions and return their state and id.
    """
    sql = """SELECT wmbs_job.id, wmbs_job_state.name AS state,
               wmbs_job.retry_count AS retry_count, wmbs_job.name AS name
                    FROM wmbs_workflow
               INNER JOIN wmbs_subscription ON
                 wmbs_workflow.id = wmbs_subscription.workflow AND
                 wmbs_subscription.subtype IN
                  (SELECT id FROM wmbs_sub_types
                   WHERE name != 'Cleanup' AND name != 'LogCollect')
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup AND
                 wmbs_job.state IN
                   (SELECT id FROM wmbs_job_state
                    WHERE name != 'complete' AND name != 'success' AND
                          name != 'cleanout' AND name != 'exhausted')
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
             WHERE wmbs_workflow.name = :workflowname"""

    def execute(self, workflowName, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"workflowname": workflowName},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
