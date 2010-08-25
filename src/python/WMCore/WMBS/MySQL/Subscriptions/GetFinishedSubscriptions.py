#!/usr/bin/env python
"""
_GetFinishedSubscriptions_

MySQL implementation of Subscription.GetFinishedSubscriptions
"""

__revision__ = "$Id: GetFinishedSubscriptions.py,v 1.3 2010/04/08 16:25:01 mnorman Exp $"
__version__ = "$Revision: 1.3 $"

import time

from WMCore.Database.DBFormatter import DBFormatter

class GetFinishedSubscriptions(DBFormatter):

    sql = """SELECT DISTINCT wmbs_sub.id FROM wmbs_subscription wmbs_sub
               INNER JOIN wmbs_fileset ON wmbs_fileset.id = wmbs_sub.fileset
               INNER JOIN (SELECT fileset, COUNT(file) AS total_files
                      FROM wmbs_fileset_files GROUP BY fileset) fileset_size ON
                      wmbs_sub.fileset = fileset_size.fileset
               LEFT OUTER JOIN (SELECT subscription, COUNT(file) AS total_files
                      FROM wmbs_sub_files_complete GROUP BY subscription) sub_complete ON
                      wmbs_sub.id = sub_complete.subscription
               LEFT OUTER JOIN (SELECT subscription, COUNT(file) AS total_files
                      FROM wmbs_sub_files_failed GROUP BY subscription) sub_failed ON
                      wmbs_sub.id = sub_failed.subscription
               WHERE wmbs_fileset.open = 0
               AND fileset_size.total_files = (COALESCE(sub_complete.total_files, 0) +
                      COALESCE(sub_failed.total_files, 0))
               AND (SELECT COUNT(wmbs_job.id) FROM wmbs_job
                      INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
                      WHERE wmbs_job.state != (SELECT id FROM wmbs_job_state WHERE name = 'cleanout')
                      AND wmbs_jobgroup.subscription = wmbs_sub.id) = 0
               AND (SELECT COUNT(wmbs_job.id) FROM wmbs_job
                      INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
                      WHERE :currTime - wmbs_job.state_time < :timeOut
                      AND wmbs_jobgroup.subscription = wmbs_sub.id) = 0
               AND (SELECT COUNT(wmbs_s1.id) FROM wmbs_subscription wmbs_s1
                      INNER JOIN wmbs_workflow_output wwo ON wwo.output_fileset = wmbs_s1.fileset
                      INNER JOIN wmbs_subscription wmbs_s2 ON wmbs_s2.workflow = wwo.workflow_id
                      WHERE wmbs_s2.id = wmbs_sub.id) = 0"""

    def execute(self, timeOut = 0, conn = None, transaction = False):
        binds = {'currTime': int(time.time()), 'timeOut': timeOut}
        results = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.formatDict(results)
