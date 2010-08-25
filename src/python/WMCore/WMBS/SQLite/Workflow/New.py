#!/usr/bin/env python
"""
_NewWorkflow_

SQLite implementation of NewWorkflow

"""
__all__ = []



from WMCore.WMBS.MySQL.Workflow.New import New as NewWorkflowMySQL

class New(NewWorkflowMySQL):
    sql = NewWorkflowMySQL.sql
    