#!/usr/bin/env python
"""
_GetWorkflowTask_

MySQL implementation of Jobs.GetWorkflowTask
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetWorkflowTask(DBFormatter):
    sql = """SELECT wmbs_workflow.name, wmbs_workflow.task, wmbs_job.id FROM wmbs_workflow
               INNER JOIN wmbs_subscription ON
                 wmbs_workflow.id = wmbs_subscription.workflow
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
             WHERE wmbs_job.id = :jobid"""    

    def execute(self, jobIDs, conn = None, transaction = False):
        binds = []
        for jobID in jobIDs:
            binds.append({"jobid": jobID})
            
        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
