#!/usr/bin/env python
"""
_LoadFromSpecOwner_

MySQL implementation of Workflow.LoadFromSpecOwner
"""

from WMCore.Database.DBFormatter import DBFormatter
    
class LoadFromSpecOwner(DBFormatter):
    sql = """SELECT id, spec, name, owner, task FROM wmbs_workflow
             WHERE spec = :spec and owner = :owner AND task = :task"""

    def execute(self, spec, owner, task, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"spec": spec, "owner": owner,
                                                 "task": task},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
