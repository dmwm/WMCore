#!/usr/bin/env python
"""
_GetInjectedWorkflows_

MySQL implementation of Workflow.GetInjectedWorkflows

"""
__all__ = []

from WMCore.Database.DBFormatter import DBFormatter

class GetInjectedWorkflows(DBFormatter):
    """
    Gets workflows that have been fully injected into WMBS
    """

    sql = """SELECT DISTINCT name FROM wmbs_workflow WHERE injected = :injected"""


    def execute(self, injected = False, conn = None, transaction = False):
        """
        _execute_

        Simple query that looks for either injected or uninjected files
        """

        if not injected:
            binds = {'injected': 0}
        else:
            binds = {'injected': 1}

        result = self.dbi.processData(self.sql, binds, conn = conn,
                                      transaction = transaction)

        dResult = self.formatDict(result)
        finalResult = []

        for entry in dResult:
            finalResult.append(entry['name'])

        return finalResult
