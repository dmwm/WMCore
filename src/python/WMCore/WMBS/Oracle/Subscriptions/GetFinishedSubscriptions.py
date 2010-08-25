#!/usr/bin/env python
"""
_GetFinishedSubscriptions_

Oracle implementation of Subscription.GetFinishedSubscriptions
"""

__all__ = []
__revision__ = "$Id: GetFinishedSubscriptions.py,v 1.2 2010/04/08 16:25:01 mnorman Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.GetFinishedSubscriptions import GetFinishedSubscriptions as MySQLFinishedSubscriptions

class GetFinishedSubscriptions(MySQLFinishedSubscriptions):
    """

    Identical to MySQL version
    """

    sql = """SELECT wmbs_subscription.id FROM wmbs_subscription
               INNER JOIN wmbs_fileset ON wmbs_fileset.id = wmbs_subscription.fileset
               INNER JOIN (SELECT fileset, COUNT(fileid) AS total_files
                      FROM wmbs_fileset_files GROUP BY fileset) fileset_size ON
                      wmbs_subscription.fileset = fileset_size.fileset
               LEFT OUTER JOIN (SELECT subscription, COUNT(fileid) AS total_files
                      FROM wmbs_sub_files_complete GROUP BY subscription) sub_complete ON
                      wmbs_subscription.id = sub_complete.subscription
               LEFT OUTER JOIN (SELECT subscription, COUNT(fileid) AS total_files
                      FROM wmbs_sub_files_failed GROUP BY subscription) sub_failed ON
                      wmbs_subscription.id = sub_failed.subscription
               WHERE wmbs_fileset.open = '0'
               AND fileset_size.total_files = (COALESCE(sub_complete.total_files, 0) +
                      COALESCE(sub_failed.total_files, 0))
               AND (SELECT COUNT(wmbs_job.id) FROM wmbs_job
                      INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
                      INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
                      WHERE wmbs_job.state != (SELECT id FROM wmbs_job_state WHERE name = 'cleanout')) = 0
               AND (SELECT COUNT(wmbs_job.id) FROM wmbs_job
                      INNER JOIN wmbs_jobgroup ON wmbs_jobgroup.id = wmbs_job.jobgroup
                      INNER JOIN wmbs_subscription ON wmbs_subscription.id = wmbs_jobgroup.subscription
                      WHERE :currTime - wmbs_job.state_time < :timeOut) = 0
               AND (SELECT COUNT(wmbs_s1.id) FROM wmbs_subscription wmbs_s1
                      INNER JOIN wmbs_workflow_output wwo ON wwo.output_fileset = wmbs_s1.fileset
                      INNER JOIN wmbs_subscription wmbs_s2 ON wmbs_s2.workflow = wwo.workflow_id
                      WHERE wmbs_s2.id = wmbs_subscription.id) = 0"""
