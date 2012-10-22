#!/usr/bin/env python
"""
_LoadFromTask_

MySQL implementation of Workflow.LoadFromTask
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class LoadFromTask(DBFormatter):
    sql = """SELECT wmbs_workflow.id, wmbs_workflow.spec, wmbs_workflow.name, wmbs_users.cert_dn as owner, wmbs_workflow.task
             FROM wmbs_workflow
             INNER JOIN wmbs_users ON
               wmbs_workflow.owner = wmbs_users.id
             WHERE wmbs_workflow.task = :task"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id attribute to an int because the DBFormatter turns everything
        into strings.
        """
        tempResults = DBFormatter.formatDict(self, result)

        formattedResults = []
        for tempResult in tempResults:
            tempResult["id"] = int(tempResult["id"])
            formattedResults.append(tempResult)

        return formattedResults

    def execute(self, task = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"task": task},
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
