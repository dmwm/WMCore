#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Workflow.LoadFromName
"""

from WMCore.Database.DBFormatter import DBFormatter


class LoadFromName(DBFormatter):
    sql = """SELECT * FROM wmbs_workflow WHERE wmbs_workflow.name = :workflow
             ORDER BY task"""

    def execute(self, workflow, conn=None, transaction=False):
        result = self.dbi.processData(self.sql, {"workflow": workflow},
                                      conn=conn, transaction=transaction)
        return self.formatDict(result)
