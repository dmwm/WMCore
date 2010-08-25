#!/usr/bin/env python
"""
_GetFinishedSubscriptions_

MySQL implementation of Subscription.GetFinishedSubscriptions
"""

__revision__ = "$Id: GetFinishedSubscriptions.py,v 1.2 2010/03/31 17:58:28 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

import time

from WMCore.Database.DBFormatter import DBFormatter

class GetFinishedSubscriptions(DBFormatter):

    sql = """SELECT wmbs_subscription.id FROM wmbs_subscription
               INNER JOIN wmbs_fileset ON wmbs_fileset.id = wmbs_subscription.fileset
               INNER JOIN (SELECT fileset, COUNT(file) AS total_files
                      FROM wmbs_fileset_files GROUP BY fileset) fileset_size ON
                      wmbs_subscription.fileset = fileset_size.fileset
               LEFT OUTER JOIN (SELECT subscription, COUNT(file) AS total_files
                      FROM wmbs_sub_files_complete GROUP BY subscription) sub_complete ON
                      wmbs_subscription.id = sub_complete.subscription
               LEFT OUTER JOIN (SELECT subscription, COUNT(file) AS total_files
                      FROM wmbs_sub_files_failed GROUP BY subscription) sub_failed ON
                      wmbs_subscription.id = sub_failed.subscription
               WHERE wmbs_fileset.open = 0
               AND fileset_size.total_files = (COALESCE(sub_complete.total_files, 0) +
                      COALESCE(sub_failed.total_files, 0))
               AND (SELECT COUNT(wmbs_job.id) FROM wmbs_job
                      INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
                      INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
                      WHERE wmbs_job.state != (SELECT id FROM wmbs_job_state WHERE name = 'cleanout')) = 0
               AND (SELECT COUNT(wmbs_job.id) FROM wmbs_job
                      INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
                      INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
                      WHERE :currTime - wmbs_job.state_time < :timeOut) = 0"""

    def execute(self, timeOut = 0, conn = None, transaction = False):
        binds = {'currTime': int(time.time()), 'timeOut': timeOut}
        results = self.dbi.processData(self.sql, binds,
                         conn = conn, transaction = transaction)
        return self.formatDict(results)
