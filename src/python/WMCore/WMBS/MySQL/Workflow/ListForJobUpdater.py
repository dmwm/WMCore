"""
__ListForJobUpdater__

MySQL implementation of ListForJobUpdater

Created on Apr 16, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class ListForJobUpdater(DBFormatter):
    """
    _ListForJobUpdater_

    List the available workflows and tasks in WMBS with the workflow
    priority and the sub task priority
    """
    sql = """SELECT wmbs_workflow.name, wmbs_workflow.task,
                    wmbs_workflow.priority AS workflow_priority,
                    MIN(wmbs_sub_types.priority) AS task_priority
             FROM wmbs_workflow
             INNER JOIN wmbs_subscription ON
               wmbs_workflow.id = wmbs_subscription.workflow
             INNER JOIN wmbs_sub_types ON
               wmbs_subscription.subtype = wmbs_sub_types.id
             GROUP BY wmbs_workflow.name, wmbs_workflow.task,
                      wmbs_workflow.priority"""

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
