#!/usr/bin/env python
"""
_CheckInjectedWorkflow_

MySQL implementation of Workflow.CheckInjectedWorkflow

"""
__all__ = []

from WMCore.Database.DBFormatter import DBFormatter

class CheckInjectedWorkflow(DBFormatter):
    """
    Checks workflows to see if they have been fully injected
    """

    sql = """SELECT injected FROM wmbs_workflow WHERE name = :name"""

    def execute(self, name, conn = None, transaction = False):
        """
        Update the workflows to match their injected status

        """
        result = self.dbi.processData(self.sql, {"name": name}, conn = conn,
                                      transaction = transaction)

        d = self.formatDict(result)[0]

        if d.get('injected', 0) == 1:
            return True
        else:
            return False
