#!/usr/bin/env python
"""
_NewWorkflow_

Oracle implementation of NewWorkflow
"""

from WMCore.WMBS.MySQL.Workflow.New import New as NewWorkflowMySQL

class New(NewWorkflowMySQL):
    sql = """insert into wmbs_workflow (id, spec, owner, name, task, type, alt_fs_close, priority)
             values (wmbs_workflow_SEQ.nextval, :spec, :owner, :name, :task, :type, :alt_fs_close, :priority)"""
