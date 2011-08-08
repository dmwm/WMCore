#!/usr/bin/env python
"""
_MarkInjectedWorkflows_

MySQL implementation of Workflow.MarkInjectedWorkflows

"""
__all__ = []

from WMCore.Database.DBFormatter import DBFormatter

class MarkInjectedWorkflows(DBFormatter):
    """
    Marks workflows that have been fully injected into WMBS
    """

    sql = """UPDATE wmbs_workflow SET injected = :injected WHERE name = :name
    """

    def execute(self, names, injected = True, conn = None,
                transaction = False):
        """
        Update the workflows to match their injected status

        """

        if injected:
            injected = 1
        else:
            injected = 0

        binds = []
        for name in names:
            binds.append({'name': name, 'injected': injected})


        self.dbi.processData(self.sql, binds, conn = conn,
                             transaction = transaction)

        return
