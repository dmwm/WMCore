#!/usr/bin/env python
"""
_LoadFromSpecOwner_

MySQL implementation of Workflow.LoadFromSpecOwner
"""

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromSpecOwner(DBFormatter):
    sql = """SELECT wmbs_workflow.id, wmbs_workflow.spec, wmbs_workflow.name,
                    wmbs_users.cert_dn as dn, wmbs_users.owner as owner,
                    wmbs_users.grp as grp, wmbs_users.group_name as vogrp,
                    wmbs_users.role_name as vorole, wmbs_workflow.task,
                    wmbs_workflow.type, wmbs_workflow.priority
             FROM wmbs_workflow
             INNER JOIN wmbs_users ON
               wmbs_workflow.owner = wmbs_users.id
             WHERE wmbs_workflow.spec = :spec AND wmbs_workflow.task = :task AND
               wmbs_users.cert_dn = :cert_dn"""

    def execute(self, spec, dn, task, conn = None,
                transaction = False):
        result = self.dbi.processData(self.sql, {"spec": spec, "cert_dn": dn,
                                                 "task": task},
                                      conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
