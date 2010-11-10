#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Workflow.LoadFromName
"""

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromName(DBFormatter):
    sql = """SELECT id, spec, name, owner, task FROM wmbs_workflow
             WHERE name = :workflow AND task = :task"""

    def execute(self, workflow, task, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"workflow": workflow,
                                                 "task": task}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
