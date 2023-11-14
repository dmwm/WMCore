#!/usr/bin/env python
"""
Module for finding out workflows with open subscriptions
for production and/or processing tasks.
"""

from WMCore.Database.DBFormatter import DBFormatter


class GetUnfinishedWorkflows(DBFormatter):
    """
    If a workflow has any production or processing type subscriptions
    still unfinished, then it is considered an unfinished/active workflow.
    """

    sql = """SELECT wmbs_workflow.name, wmbs_workflow.spec
               FROM wmbs_workflow
               INNER JOIN wmbs_subscription ON wmbs_subscription.workflow = wmbs_workflow.id
               INNER JOIN wmbs_sub_types ON wmbs_sub_types.id = wmbs_subscription.subtype
               WHERE wmbs_subscription.finished = 0
                 AND wmbs_sub_types.name IN ('Processing', 'Production') 
               GROUP BY wmbs_workflow.name, wmbs_workflow.spec"""

    def execute(self, conn=None, transaction=False):
        """
        Executes the SQL statement above.
        :param conn: connection object to the database
        :param transaction: boolean defining whether it's part of a transaction or not
        :return: a list of dictionary elements
        """
        result = self.dbi.processData(self.sql, conn=conn, transaction=transaction)
        return self.formatDict(result)
