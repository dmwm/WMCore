#!/usr/bin/env python
"""
_NewWorkflow_

Oracle implementation of NewWorkflow
"""

from WMCore.WMBS.MySQL.Workflow.New import New as NewWorkflowMySQL

class New(NewWorkflowMySQL):
    sql = """insert into wmbs_workflow (spec, owner, name, task, type, alt_fs_close, priority)
              values (:spec, :owner, :name, :task, :type, :alt_fs_close, :priority)"""
