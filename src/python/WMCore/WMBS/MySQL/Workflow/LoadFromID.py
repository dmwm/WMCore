#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Workflow.LoadFromID
"""

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = "SELECT id, spec, name, owner, task FROM wmbs_workflow WHERE id = :workflow"
                                    
    def execute(self, workflow = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"workflow": workflow}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
