#!/usr/bin/env python
"""
_GetFinishedTasks_

MySQL implementation of Workflow.GetFinishedTasks

Created on Nov 7, 2012

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class GetFinishedTasks(DBFormatter):
    """
    _GetFinishedTasks_

    Returns those tasks where the associated subscriptions
    are marked as finished
    """

    sql = """SELECT wmbs_workflow.name, wmbs_workflow.task,
                    wmbs_workflow.spec
             FROM wmbs_workflow
             LEFT OUTER JOIN wmbs_subscription ON
                 wmbs_subscription.workflow = wmbs_workflow.id AND
                 wmbs_subscription.finished = 0
            GROUP BY wmbs_workflow.name, wmbs_workflow.task,
                     wmbs_workflow.spec
            HAVING COUNT(wmbs_subscription.id) = 0
          """

    def execute(self, conn = None, transaction = False):
        """
        _execute_

        Returns a list of dictionaries with the following structure:
        [ {'name' : <requestName>, 'task' : <taskPath>}, ...]
        """
        result = self.dbi.processData(self.sql,
                                        conn = conn, transaction = transaction)
        return self.formatDict(result)
