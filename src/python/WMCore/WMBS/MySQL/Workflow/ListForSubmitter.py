"""
__ListForSubmitter__

MySQL implementation of ListForSubmitter

Created on Apr 17, 2013

@author: dballest
"""

from WMCore.Database.DBFormatter import DBFormatter

class ListForSubmitter(DBFormatter):
    """
    _ListForSubmitter_

    List the available workflows in WMBS with the workflow
    priority
    """
    sql = """SELECT wmbs_workflow.name AS name,
                    MIN(wmbs_workflow.priority) AS priority
             FROM wmbs_workflow
             GROUP BY wmbs_workflow.name"""

    def execute(self, conn = None, transaction = False):
        result = self.dbi.processData(self.sql,
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)
