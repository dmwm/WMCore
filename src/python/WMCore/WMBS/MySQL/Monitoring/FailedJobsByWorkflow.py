#!/usr/bin/env python
"""
_FailedJobsByWorkflow_

MySQL implementation of Workflow.FailedJobs
"""

from WMCore.Database.DBFormatter import DBFormatter

class FailedJobsByWorkflow(DBFormatter):
    sql = """SELECT ww.id, ww.name, ww.task, wj.id as job_id
                    FROM wmbs_workflow ww
               INNER JOIN wmbs_subscription ws ON
                 ww.id = ws.workflow
               LEFT OUTER JOIN wmbs_jobgroup wjg ON
                 ws.id = wjg.subscription
               LEFT OUTER JOIN wmbs_job wj ON
                 wjg.id = wj.jobgroup
               LEFT OUTER JOIN wmbs_job_state wjs ON
                 wj.state = wjs.id
             WHERE wj.outcome = 0 AND
                   (wjs.name = 'exhausted' OR
                    wjs.name = 'cleanout') AND
                    ww.name = :workflowName
             ORDER BY ww.id"""

    def execute(self, workflowName, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {'workflowName': workflowName},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
