#!/usr/bin/env python
"""
_TaskSummary_

List the summary of job numbers by task given a workflow 
"""

__revision__ = "$Id: TaskSummaryByWorkflow.py,v 1.1 2010/08/16 22:23:57 sryu Exp $"
__version__  = "$Revision: 1.1 $"

from WMCore.Database.DBFormatter import DBFormatter

class TaskSummaryByWorkflow(DBFormatter):
    sql = """SELECT wmbs_workflow.id, wmbs_workflow.name AS wm_spec, 
                    wmbs_workflow.task, 
                    COUNT(wmbs_job.id) AS num_job, wmbs_job_state.name,
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
            ORDER BY wmbs_workflow.id DESC"""
            
    def formatWorkflow(self, results):
        workflow = {}
        tran = Transitions()
        for result in results:
            if not workflow.has_key(result["task"]):
                workflow[result["task"]] = {}
                for state in tran.states():
                    workflow[result["task"]][state] = 0
                    
                workflow[result["task"]][result["state"]] = result["num_job"]
                workflow[result["task"]]["num_task"] = result["num_task"]
                workflow[result["task"]]["real_success"] = int(result["success"])
                workflow[result["task"]]["id"] = result["id"]
                workflow[result["task"]]["wmspec"] = result["wmspec"] 
            else:
                workflow[result["task"]][result["state"]] = result["num_job"]
                workflow[result["task"]]["num_task"] += result["num_task"]
                workflow[result["task"]]["real_success"] += int(result["success"])
        
        # need to order by id (client side)        
        return workflow.values()

    def execute(self, workflowName, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {'workflow_name': workflowName},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)