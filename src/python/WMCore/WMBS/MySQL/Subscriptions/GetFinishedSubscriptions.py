#!/usr/bin/env python
"""
_GetFinishedSubscriptions_

MySQL implementation of Subscription.GetFinishedSubscriptions
"""




import time
import logging

from WMCore.Database.DBFormatter import DBFormatter

class GetFinishedSubscriptions(DBFormatter):
    sql = """SELECT DISTINCT wmbs_sub.id FROM wmbs_subscription wmbs_sub
               INNER JOIN wmbs_fileset ON wmbs_fileset.id = wmbs_sub.fileset
               LEFT OUTER JOIN (SELECT fileset, COUNT(DISTINCT file) AS total_files
                      FROM wmbs_fileset_files GROUP BY fileset) fileset_size ON
                      wmbs_sub.fileset = fileset_size.fileset
               LEFT OUTER JOIN (SELECT subscription, COUNT(DISTINCT file) AS total_files
                      FROM wmbs_sub_files_complete GROUP BY subscription) sub_complete ON
                      wmbs_sub.id = sub_complete.subscription
               LEFT OUTER JOIN (SELECT subscription, COUNT(DISTINCT file) AS total_files
                      FROM wmbs_sub_files_failed GROUP BY subscription) sub_failed ON
                      wmbs_sub.id = sub_failed.subscription
               LEFT OUTER JOIN
                 (SELECT wmbs_subscription.id, COUNT(*) AS total FROM wmbs_subscription
                    LEFT OUTER JOIN wmbs_workflow_output ON
                      wmbs_subscription.workflow = wmbs_workflow_output.workflow_id
                    INNER JOIN wmbs_subscription child_subscriptions ON
                      wmbs_workflow_output.output_fileset = child_subscriptions.fileset
                  GROUP BY wmbs_subscription.id) child_subscriptions ON
                 wmbs_sub.id = child_subscriptions.id     
               WHERE wmbs_fileset.open = 0 AND child_subscriptions.total IS Null
               AND COALESCE(fileset_size.total_files, 0) =
                     (COALESCE(sub_complete.total_files, 0) +
                      COALESCE(sub_failed.total_files, 0))
               AND (SELECT COUNT(wmbs_job.id) FROM wmbs_job
                      INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
                      WHERE wmbs_job.state != (SELECT id FROM wmbs_job_state WHERE name = 'cleanout')
                      AND wmbs_jobgroup.subscription = wmbs_sub.id) = 0
               AND (SELECT COUNT(wmbs_job.id) FROM wmbs_job
                      INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
                      WHERE :currTime - wmbs_job.state_time < :timeOut
                      AND wmbs_jobgroup.subscription = wmbs_sub.id) = 0"""

    def execute(self, timeOut = 0, conn = None, transaction = False):
        binds = {'currTime': int(time.time()), 'timeOut': timeOut}
        results = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.formatDict(results)
