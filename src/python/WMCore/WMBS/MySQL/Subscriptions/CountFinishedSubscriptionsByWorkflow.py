"""
_CountFinishedSubscriptionsByWorkflow_

MySQL implementation of Subscription.CountFinishedSubscriptionsByWorkflow
"""
from __future__ import division, print_function

from builtins import str, bytes

from WMCore.Database.DBFormatter import DBFormatter


class CountFinishedSubscriptionsByWorkflow(DBFormatter):
    """
    Gets count subscriptions given workflow but exculding cleanup and log collect subscription
    """

    sql = """SELECT ww.name as workflow,
             COUNT(case when ws.finished = 1 then 1 else null end) as finished,
             COUNT(case when ws.finished = 0 then 1 else null end) as open
             FROM wmbs_subscription ws
             INNER JOIN wmbs_sub_types wst ON wst.id = ws.subtype
             INNER JOIN wmbs_workflow ww ON ww.id = ws.workflow
             WHERE wst.name NOT IN ('LogCollect', 'Cleanup') AND ww.name = :workflowName
             GROUP BY ww.name
          """

    def execute(self, workflowNames, conn=None, transaction=False):
        """
        _execute_

        This DAO returns a list of dictionaries containing
        the key 'id' with the id of the finished subscriptions
        """
        if isinstance(workflowNames, (str, bytes)):
            workflowNames = [workflowNames]

        binds = [{'workflowName': w} for w in workflowNames]

        if not binds:
            return []

        result = self.dbi.processData(self.sql, binds, conn=conn, transaction=transaction)
        return self.formatDict(result)
