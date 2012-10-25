#!/usr/bin/env python
"""
_GetAndMarkNewFinishedSubscriptions_

MySQL implementation of Subscriptions.GetAndMarkNewFinishedSubscriptions
"""

import time

from WMCore.Database.DBFormatter import DBFormatter

class GetAndMarkNewFinishedSubscriptions(DBFormatter):
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
    sql = """UPDATE wmbs_subscription
             SET wmbs_subscription.finished = 1, wmbs_subscription.last_update = :timestamp
             WHERE wmbs_subscription.id IN (
               SELECT id FROM (
                 SELECT complete_subscription.id
                 FROM ( SELECT wmbs_subscription.id,
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
                            LEFT OUTER JOIN wmbs_jobgroup ON
                                wmbs_jobgroup.subscription = wmbs_subscription.id
                            LEFT OUTER JOIN wmbs_job ON
                                wmbs_job.jobgroup = wmbs_jobgroup.id AND
                                wmbs_job.state_time > :maxTime AND
                                wmbs_job.state != %d
                        WHERE wmbs_subscription.finished = 0
                        GROUP BY wmbs_subscription.id,
                                 wmbs_subscription.fileset,
                                 wmbs_workflow.name
                        HAVING COUNT(wmbs_sub_files_available.subscription) = 0
                        AND COUNT(wmbs_sub_files_acquired.subscription) = 0
                        AND COUNT(wmbs_job.id) = 0 ) complete_subscription
                     INNER JOIN wmbs_fileset ON
                         wmbs_fileset.id = complete_subscription.fileset
                     LEFT OUTER JOIN wmbs_fileset_files ON
                         wmbs_fileset_files.fileset = wmbs_fileset.id
                     LEFT OUTER JOIN wmbs_file_parent ON
                         wmbs_file_parent.parent = wmbs_fileset_files.fileid
                     LEFT OUTER JOIN wmbs_fileset_files child_fileset ON
                         child_fileset.fileid = wmbs_file_parent.child
                     LEFT OUTER JOIN wmbs_subscription child_subscription ON
                         child_subscription.fileset = child_fileset.fileset AND
                         child_subscription.finished = 0
                     LEFT OUTER JOIN wmbs_workflow child_workflow ON
                         child_subscription.workflow = child_workflow.id AND
                         child_workflow.name != complete_subscription.name
                 GROUP BY complete_subscription.id
                 HAVING COUNT(child_workflow.name) = 0 ) deletable_subscriptions )
             """

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
