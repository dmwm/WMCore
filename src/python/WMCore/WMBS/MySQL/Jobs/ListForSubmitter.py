#!/usr/bin/env python
"""
_ListForSubmitter_

MySQL function to list jobs for submission
"""

from WMCore.Database.DBFormatter import DBFormatter


class ListForSubmitter(DBFormatter):
    """
    _ListForSubmitter_

    List the available jobs in WMBS order by descending subscription priority,
    descending workflow priority, and ascending workflow ID.
    """
    sql = """SELECT wmbs_job.id AS id,
                    wmbs_job.name AS name,
                    wmbs_job.cache_dir AS cache_dir,
                    wmbs_sub_types.name AS task_type,
                    wmbs_sub_types.priority AS task_prio,
                    wmbs_job.retry_count AS retry_count,
                    wmbs_workflow.name AS request_name,
                    wmbs_workflow.id AS task_id,
                    wmbs_workflow.priority AS wf_priority,
                    wmbs_workflow.task AS task_name
               FROM wmbs_job
               INNER JOIN wmbs_jobgroup ON
                 wmbs_job.jobgroup = wmbs_jobgroup.id
               INNER JOIN wmbs_subscription ON
                 wmbs_jobgroup.subscription = wmbs_subscription.id
               INNER JOIN wmbs_sub_types ON
                 wmbs_subscription.subtype = wmbs_sub_types.id
               INNER JOIN wmbs_job_state ON
                 wmbs_job.state = wmbs_job_state.id
               INNER JOIN wmbs_workflow ON
                 wmbs_subscription.workflow = wmbs_workflow.id
             WHERE wmbs_job_state.name = 'created'
             ORDER BY
               wmbs_sub_types.priority DESC,
               wmbs_workflow.priority DESC,
               wmbs_workflow.id ASC"""

    limit_sql = " limit %d"

    def execute(self, conn=None, transaction=False, limitRows=None):
        if limitRows:
            extraSql = self.limit_sql % limitRows
        else:
            extraSql = ""

        result = self.dbi.processData(self.sql + extraSql, conn=conn,
                                      transaction=transaction)
        return self.formatDict(result)
