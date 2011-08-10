#!/usr/bin/env python
"""
_NewWorkflow_

MySQL implementation of NewWorkflow
"""

from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    """
    Create a workflow ready for subscriptions
    """
    sql = """insert into wmbs_workflow (spec, owner, name, task, type)
                values (:spec, :owner, :name, :task, :type)"""
    
    def execute(self, spec = None, owner = None, name = None, task = None,
                wfType = None, conn = None, transaction = False):
        binds = {"spec": spec, "owner": owner, "name": name, "task": task,
                 "type": wfType}
        self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        return
