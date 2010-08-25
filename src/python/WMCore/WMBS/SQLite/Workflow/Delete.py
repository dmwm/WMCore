#!/usr/bin/env python
"""
_DeleteWorkflow_

SQLite implementation of DeleteWorkflow

"""
__all__ = []



from WMCore.WMBS.MySQL.Workflow.Delete import Delete as DeleteWorkflowMySQL

class Delete(DeleteWorkflowMySQL):
    sql = DeleteWorkflowMySQL.sql
    