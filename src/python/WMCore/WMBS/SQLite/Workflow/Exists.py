#!/usr/bin/env python
"""
_Exists_

SQLite implementation of WorkflowExists

"""
__all__ = []



from WMCore.WMBS.MySQL.Workflow.Exists import Exists as WorkflowExistsMySQL

class Exists(WorkflowExistsMySQL):
    sql = WorkflowExistsMySQL.sql