#!/usr/bin/env python
"""
_NewWorkflow_

Oracle implementation of NewWorkflow

"""
__all__ = []



from WMCore.WMBS.MySQL.Workflow.New import New as NewWorkflowMySQL

class New(NewWorkflowMySQL):
    sql = """insert into wmbs_workflow (id, spec, owner, name, task)
             values (wmbs_workflow_SEQ.nextval, :spec, :owner, :name, :task)"""
    