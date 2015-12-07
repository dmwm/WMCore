"""
_ReleasePeriodicJob_

MySQL implementation of JobSplitting.ReleasePeriodicJob
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class ReleasePeriodicJob(DBFormatter):
    """
    _ReleasePeriodicJob_

    Returns either 0 (do not release the next periodic harvesting job)
                or 1 (release the next periodic harvesting job).

    Existence of available files is assumed for this query, because
    a job splitting algorithm is only called if there are
    available files for the subscription.

    To return 1, we need to either have no jobs (jobgroups) or all
    existing jobs need to have finished and their latest state
    change must have occured longer than 'period' seconds ago.
    """

    sql = """SELECT CASE
                      WHEN COUNT(wmbs_jobgroup.id) = 0 THEN 1
                      WHEN COUNT(wmbs_sub_files_acquired.subscription) > 0 THEN 0
                      WHEN COUNT(wmbs_job.id) = 0 THEN 1
                      ELSE 0
                    END
             FROM wmbs_subscription
             LEFT OUTER JOIN wmbs_jobgroup ON
               wmbs_jobgroup.subscription = wmbs_subscription.id
             LEFT OUTER JOIN wmbs_sub_files_acquired ON
               wmbs_sub_files_acquired.subscription = wmbs_subscription.id
             LEFT OUTER JOIN wmbs_job ON
               wmbs_job.jobgroup = wmbs_jobgroup.id AND
               wmbs_job.state_time > :TIMEOUT
             WHERE wmbs_subscription.id = :subscription
             """

    def execute(self, subscription, period, conn = None, transaction = False):

        binds = { 'SUBSCRIPTION' : subscription,
                  'TIMEOUT' : int(time.time()) - period }

        release = self.dbi.processData(self.sql, binds, conn = conn,
                                       transaction = transaction)[0].fetchall()[0][0]

        return (release == 1)
