#!/usr/bin/env python
"""
_MarkNewFinishedSubscriptions_

MySQL implementation of SubscriptionsMarkNewFinishedSubscriptions
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class MarkNewFinishedSubscriptions(DBFormatter):
    """

    Searches for all subscriptions where the fileset is closed,
    the workflow is injected, there are no available files,
    no acquired files, no unfinished child subscriptions, no jobs that
    are not in state 'cleanout' and whose state has not changed in some time  and
    where the state is not finished. Also checks that the files in the input fileset
    are not parents of files associated with a unfinished subscription, possibly in another
    workflow.

    It marks such subscriptions and finished
    """

    completeNonJobSQL = """
                    SELECT distinct wmbs_subscription.id,
                                    wmbs_subscription.fileset,
                                    wmbs_workflow.name
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
                     WHERE wmbs_subscription.finished = 0 AND
                            wmbs_sub_files_available.subscription is Null AND
                            wmbs_sub_files_acquired.subscription is Null
                    """
    subWithUnfinishedJobSQL = """
                    SELECT distinct wmbs_subscription.id
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

                        INNER JOIN wmbs_jobgroup ON
                            wmbs_jobgroup.subscription = wmbs_subscription.id
                        INNER JOIN wmbs_job ON
                            wmbs_job.jobgroup = wmbs_jobgroup.id AND
                            wmbs_job.state_time > :maxTime AND
                            wmbs_job.state != %d
                     WHERE wmbs_subscription.finished = 0 AND
                        wmbs_sub_files_available.subscription is Null AND
                        wmbs_sub_files_acquired.subscription is Null
                  """

    sql = """ UPDATE wmbs_subscription
             SET wmbs_subscription.finished = 1, wmbs_subscription.last_update = :timestamp
             WHERE wmbs_subscription.id IN (
             SELECT id FROM (
                    SELECT complete_subscription.id
                        FROM ( %s ) complete_subscription
                    WHERE complete_subscription.id
                        NOT IN ( %s )
                    GROUP BY complete_subscription.id) deletable_subscriptions )""" % (
                                                        completeNonJobSQL, subWithUnfinishedJobSQL)


    def execute(self, state, timeOut = None, conn = None, transaction = False):

        currentTime = int(time.time())
        if timeOut == None:
            timeOut = currentTime

        binds = {'maxTime'   : currentTime - timeOut,
                 'timestamp' : currentTime}

        self.dbi.processData(self.sql % state,
                             binds, conn = conn,
                             transaction = transaction)

        return

