#!/usr/bin/env python
"""
_GetFinishedSubscriptions_

MySQL implementation of Subscription.GetFinishedSubscriptions
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class GetFinishedSubscriptions(DBFormatter):
    """

    Searches for all subscriptions where the fileset is closed,
    the workflow is injected, there are no available files,
    no acquired files, no child subscriptions, no jobs that
    are not in state 'cleanout' and no jobs whose last state
    change was not been at least a certain time ago

    """
    sql = """SELECT wmbs_subscription.id
             FROM wmbs_subscription
               INNER JOIN wmbs_fileset ON
                 wmbs_fileset.id = wmbs_subscription.fileset AND
                 wmbs_fileset.open = 0
               INNER JOIN wmbs_workflow ON
                 wmbs_workflow.id = wmbs_subscription.workflow AND
                 wmbs_workflow.injected = 1
               LEFT OUTER JOIN wmbs_sub_files_available ON
                 wmbs_sub_files_available.subscription = wmbs_subscription.id
               LEFT OUTER JOIN wmbs_sub_files_acquired ON
                 wmbs_sub_files_acquired.subscription = wmbs_subscription.id
               LEFT OUTER JOIN wmbs_workflow_output ON
                 wmbs_workflow_output.workflow_id = wmbs_subscription.workflow
               LEFT OUTER JOIN wmbs_subscription child_subscription ON
                 child_subscription.fileset = wmbs_workflow_output.output_fileset
               LEFT OUTER JOIN wmbs_jobgroup ON
                 wmbs_jobgroup.subscription = wmbs_subscription.id
               LEFT OUTER JOIN wmbs_job ON
                 wmbs_job.jobgroup = wmbs_jobgroup.id AND
                 wmbs_job.state_time > :currTime - :timeOut
               LEFT OUTER JOIN wmbs_job_state ON
                 wmbs_job_state.id = wmbs_job.state AND
                 wmbs_job_state.name != 'cleanout'
             GROUP BY wmbs_subscription.id
             HAVING COUNT(wmbs_sub_files_available.subscription) = 0
             AND COUNT(wmbs_sub_files_acquired.subscription) = 0
             AND COUNT(child_subscription.fileset) = 0
             AND COUNT(wmbs_job_state.id) = 0
             ORDER BY wmbs_subscription.id DESC
             """

    def execute(self, timeOut = 0, conn = None, transaction = False):

        binds = { 'currTime' : int(time.time()),
                  'timeOut' : timeOut }

        results = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)

        return self.formatDict(results)
