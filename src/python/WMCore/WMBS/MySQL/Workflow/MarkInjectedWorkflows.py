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
    selectSQL = """SELECT spec FROM wmbs_workflow WHERE name = :name"""
    updateSQL = """UPDATE wmbs_workflow SET injected = :injected WHERE spec = :spec"""


    def execute(self, names, injected = True, conn = None,
                transaction = False):
        """
        Update the workflows to match their injected status

        """

        if injected:
            injected = 1
        else:
            injected = 0

        # Get the specs
        binds = []
        for name in names:
            binds.append({'name': name})

        result = self.dbi.processData(self.selectSQL, binds, conn = conn,
                                      transaction = transaction)

        # Use specs to update table
        l = self.formatDict(result)
        binds = []
        for entry in l:
            binds.append({'spec': entry['spec'], 'injected': injected})


        self.dbi.processData(self.updateSQL, binds, conn = conn,
                             transaction = transaction)

        return
