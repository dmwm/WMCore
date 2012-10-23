#!/usr/bin/env python
"""
_CountWorkflowBySpec_

MySQL implementation of Workflow.CountWorkflowBySpec
"""

from WMCore.Database.DBFormatter import DBFormatter

class CountWorkflowBySpec(DBFormatter):
    sql = """SELECT COUNT(*) AS workflows FROM wmbs_workflow WHERE spec = :spec"""

    def execute(self, spec, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"spec": spec},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)[0].get('workflows', 0)
