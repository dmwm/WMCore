#!/usr/bin/env python
"""
_WorkflowSummary_

List the task name and number of jobs running for a given site and subscription
type.
"""


from WMCore.Database.DBFormatter import DBFormatter
from WMCore.JobStateMachine.Transitions import Transitions

from future.utils import listvalues

class WorkflowSummary(DBFormatter):
    sql = """SELECT MAX(wmbs_workflow.id) AS id, wmbs_workflow.name AS wmspec,
                    COUNT(wmbs_job.id) AS num_job,
                    SUM(wmbs_job.outcome) AS success, wmbs_job_state.name AS state
             FROM wmbs_workflow
               INNER JOIN wmbs_subscription ON
                 wmbs_workflow.id = wmbs_subscription.workflow
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
            GROUP BY wmbs_workflow.name, wmbs_job_state.name
            ORDER BY id DESC"""

    def failCount(self, result):
        if  result["state"] == 'success' or result["state"] == 'cleanout' \
            or result["state"] == 'exhausted':
            return (result["num_job"] - int(result["success"]))
        return 0

    def pendingCount(self, result):
        if  result["state"] == 'none' or result["state"] == 'new':
            return (result["num_job"] - int(result["success"]))
        return 0

    def processingCount(self, result):

        if  result["state"] != 'success' and result["state"] != 'cleanout' \
            and result["state"] != 'exhausted' and result['state'] != 'none' \
            and result["state"] != 'new':
            return result["num_job"]
        else:
            return 0

    def formatWorkflow(self, results):
        workflow = {}
        tran = Transitions()
        for result in results:
            if result["wmspec"] not in workflow:
                workflow[result["wmspec"]] = {}
                for state in tran.states():
                    workflow[result["wmspec"]][state] = 0

                workflow[result["wmspec"]][result["state"]] = result["num_job"]
                workflow[result["wmspec"]]['total_jobs'] = result["num_job"]
                workflow[result["wmspec"]]["real_success"] = int(result["success"])
                workflow[result["wmspec"]]["id"] = result["id"]
                workflow[result["wmspec"]]["wmspec"] = result["wmspec"]
                workflow[result["wmspec"]]["pending"] = self.pendingCount(result)
                workflow[result["wmspec"]]["real_fail"] = self.failCount(result)
                workflow[result["wmspec"]]['processing'] = self.processingCount(result)
            else:
                workflow[result["wmspec"]][result["state"]] = result["num_job"]
                workflow[result["wmspec"]]['total_jobs'] += result["num_job"]
                workflow[result["wmspec"]]["real_success"] += int(result["success"])
                workflow[result["wmspec"]]["pending"] += self.pendingCount(result)
                workflow[result["wmspec"]]["real_fail"] += self.failCount(result)
                workflow[result["wmspec"]]['processing'] += self.processingCount(result)

        # need to order by id (client side)
        return listvalues(workflow)

    def execute(self, conn = None, transaction = False):
        results = self.dbi.processData(self.sql,
                                       conn = conn, transaction = transaction)
        return self.formatWorkflow(self.formatDict(results))
