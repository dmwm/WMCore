"""
_GetSemiFinishedTasks_

MySQL implementation of Subscriptions.GetSemiFinishedTasks

Created on Nov 7, 2012

@author: dballest
"""

from time import time

from WMCore.Database.DBFormatter import DBFormatter

class GetSemiFinishedTasks(DBFormatter):
    """
    _GetSemiFinishedTasks_

    In some particular cases (e.g. Repack) the workflows
    can't be marked as injected for some time which prevents
    the GetAndMarkNewFinishedSubscriptions DAO from picking actually finished
    subscriptions. This DAO is a more relaxed version of the former, but it doesn't mark
    anything and returns tasks instead of subscriptions.

    The relaxation is present in the following checks:

    - Not injected workflows are admitted
    - Doesn't check for files which are shared with other workflows

    It also allows a pattern to filter in the workflow name
    """

    sql = """SELECT complete_subscriptions.name, complete_subscriptions.task,
                    complete_subscriptions.spec
             FROM
                (SELECT wmbs_subscription.id AS sub_id,
                        wmbs_workflow.task AS task,
                        wmbs_workflow.name AS name,
                        wmbs_workflow.id AS workflow,
                        wmbs_workflow.spec AS spec
                 FROM wmbs_subscription
                 INNER JOIN wmbs_fileset ON
                     wmbs_fileset.id = wmbs_subscription.fileset AND
                     wmbs_fileset.open = 0
                 INNER JOIN wmbs_workflow ON
                     wmbs_workflow.id = wmbs_subscription.workflow AND
                     wmbs_workflow.name LIKE :pattern
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
                 GROUP BY wmbs_workflow.task,
                          wmbs_workflow.name,
                          wmbs_workflow.spec,
                          wmbs_workflow.id,
                          wmbs_subscription.id
                 HAVING COUNT(wmbs_sub_files_available.subscription) = 0
                 AND COUNT(wmbs_sub_files_acquired.subscription) = 0
                 AND COUNT(wmbs_job.id) = 0 ) complete_subscriptions
             RIGHT OUTER JOIN wmbs_subscription ON
                 wmbs_subscription.workflow = complete_subscriptions.workflow
             GROUP BY complete_subscriptions.name, complete_subscriptions.task,
                      complete_subscriptions.spec, complete_subscriptions.workflow
             HAVING COUNT(complete_subscriptions.sub_id) = COUNT(wmbs_subscription.id)
          """

    def execute(self, state, pattern = '%Repack%', timeOut = None, conn = None, transaction = False):

        currentTime = int(time())
        if timeOut == None:
            timeOut = currentTime

        binds = {'maxTime'   : currentTime - timeOut,
                 'pattern'   : pattern}

        result = self.dbi.processData(self.sql % state,
                                      binds, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
