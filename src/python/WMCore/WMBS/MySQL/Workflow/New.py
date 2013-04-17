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
    sql = """insert into wmbs_workflow (spec, owner, name, task, type, alt_fs_close, priority)
                values (:spec, :owner, :name, :task, :type, :alt_fs_close, :priority)"""

    def execute(self, spec = None, owner = None, name = None, task = None,
                wfType = None, alt_fs_close = False, priority = None,
                conn = None, transaction = False):
        binds = {"spec": spec, "owner": owner, "name": name, "task": task,
                 "type": wfType, "alt_fs_close": int(alt_fs_close), "priority" : priority}
        self.dbi.processData(self.sql, binds, conn = conn, transaction = transaction)
        return
