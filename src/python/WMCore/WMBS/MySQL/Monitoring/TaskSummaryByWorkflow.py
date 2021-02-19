#!/usr/bin/env python
"""
_TaskSummary_

List the summary of job numbers by task given a workflow
"""

from WMCore.Database.DBFormatter import DBFormatter
from WMCore.JobStateMachine.Transitions import Transitions

from future.utils import listvalues

class TaskSummaryByWorkflow(DBFormatter):
    sql = """SELECT wmbs_workflow.id, wmbs_workflow.name AS wmspec,
                    wmbs_workflow.task,
                    COUNT(wmbs_job.id) AS num_job, wmbs_job_state.name AS state,
                    SUM(wmbs_job.outcome) AS success
             FROM wmbs_workflow
               INNER JOIN wmbs_subscription ON
                 wmbs_workflow.id = wmbs_subscription.workflow
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
            WHERE wmbs_workflow.name = :workflow_name
            GROUP BY wmbs_workflow.task, wmbs_job_state.name
            ORDER BY wmbs_workflow.id ASC"""

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
            if result["task"] not in workflow:
                workflow[result["task"]] = {}
                for state in tran.states():
                    workflow[result["task"]][state] = 0

                workflow[result["task"]][result["state"]] = result["num_job"]
                workflow[result["task"]]['total_jobs'] = result["num_job"]
                workflow[result["task"]]["real_success"] = int(result["success"])
                workflow[result["task"]]["id"] = result["id"]
                workflow[result["task"]]["wmspec"] = result["wmspec"]
                workflow[result["task"]]["task"] = result["task"]
                workflow[result["task"]]["real_fail"] = self.failCount(result)
                workflow[result["task"]]['processing'] = self.processingCount(result)
            else:
                workflow[result["task"]][result["state"]] = result["num_job"]
                workflow[result["task"]]['total_jobs'] += result["num_job"]
                workflow[result["task"]]["real_success"] += int(result["success"])
                workflow[result["task"]]["real_fail"] += self.failCount(result)
                workflow[result["task"]]['processing'] += self.processingCount(result)

        # need to order by id (client side)
        return listvalues(workflow)

    def execute(self, workflowName, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {'workflow_name': workflowName},
                                       conn = conn, transaction = transaction)
        return self.formatWorkflow(self.formatDict(results))
