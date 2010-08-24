#!/usr/bin/env python
"""
_ListWorkloadsForTaskSite_

List the task name and number of jobs running for a given site and subscription
type.
"""




from WMCore.Database.DBFormatter import DBFormatter

class ListWorkloadsForTaskSite(DBFormatter):
    sql = """SELECT wmbs_workflow.task AS task, COUNT(wmbs_job.id) AS running FROM wmbs_workflow
               INNER JOIN wmbs_subscription ON
                 wmbs_workflow.id = wmbs_subscription.workflow
               INNER JOIN wmbs_sub_types ON
                 wmbs_subscription.subtype = wmbs_sub_types.id
               INNER JOIN wmbs_jobgroup ON
                 wmbs_subscription.id = wmbs_jobgroup.subscription
               INNER JOIN wmbs_job ON
                 wmbs_jobgroup.id = wmbs_job.jobgroup
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
               INNER JOIN wmbs_location ON
                 wmbs_job.location = wmbs_location.id
             WHERE wmbs_job_state.name = 'executing' AND
                   wmbs_sub_types.name = :task_type AND
                   wmbs_location.site_name = :site_name
             GROUP BY wmbs_workflow.task"""

    def execute(self, taskType, siteName, conn = None, transaction = False):
        results = self.dbi.processData(self.sql, {"task_type": taskType,
                                                  "site_name": siteName},
                                       conn = conn, transaction = transaction)
        return self.formatDict(results)
