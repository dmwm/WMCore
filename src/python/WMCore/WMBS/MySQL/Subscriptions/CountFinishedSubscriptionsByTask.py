"""
_CountFinishedSubscriptionsByTask_

MySQL implementation of Subscription.CountFinishedSubscriptionsByTask
"""

from WMCore.Database.DBFormatter import DBFormatter

class CountFinishedSubscriptionsByTask(DBFormatter):
    """
    Gets count subscriptions given task
    """

    sql = """SELECT ww.name as workflow, ww.task as task, wst.name as jobtype,
             COUNT(case when ws.finished = 1 then 1 else null end) as finished,
             COUNT(case when ws.finished = 0 then 1 else null end) as open,
             COUNT(ws.id) as total,
             MAX(ws.last_update) as updated
             FROM wmbs_subscription ws
             INNER JOIN wmbs_sub_types wst ON wst.id = ws.subtype
             INNER JOIN wmbs_workflow ww ON ww.id = ws.workflow
             GROUP BY ww.name, ww.task, wst.name
          """

    def execute(self, conn = None, transaction = False):
        """
        _execute_

        This DAO returns a list of dictionaries containing
        the key 'id' with the id of the finished subscriptions
        """

        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
