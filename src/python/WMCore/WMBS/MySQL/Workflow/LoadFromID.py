#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Workflow.LoadFromID
"""

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """SELECT wmbs_workflow.id, wmbs_workflow.spec, wmbs_workflow.name,
                    wmbs_users.name_hn as owner, wmbs_workflow.task,
                    wmbs_workflow.type
             FROM wmbs_workflow
             INNER JOIN wmbs_users ON
               wmbs_workflow.owner = wmbs_users.id
             WHERE wmbs_workflow.id = :workflow"""
                                    
    def execute(self, workflow = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"workflow": workflow}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
