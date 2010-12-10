#!/usr/bin/env python
"""
_LoadFromSpecOwner_

MySQL implementation of Workflow.LoadFromSpecOwner
"""

from WMCore.Database.DBFormatter import DBFormatter
    
class LoadFromSpecOwner(DBFormatter):
    sql = """SELECT wmbs_workflow.id, wmbs_workflow.spec, wmbs_workflow.name, wmbs_users.cert_dn as owner, wmbs_workflow.task
             FROM wmbs_workflow
             INNER JOIN wmbs_users ON
               wmbs_workflow.owner = wmbs_users.id
             WHERE wmbs_workflow.spec = :spec AND wmbs_workflow.task = :task AND
               wmbs_users.cert_dn = :owner"""

    def execute(self, spec, owner, task, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"spec": spec, "owner": owner,
                                                 "task": task},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
