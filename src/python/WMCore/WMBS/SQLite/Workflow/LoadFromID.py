#!/usr/bin/env python
"""
_LoadFromID_

SQLite implementation of Workflow.LoadFromID

"""
__all__ = []



from WMCore.WMBS.MySQL.Workflow.LoadFromID import LoadFromID as LoadWorkflowMySQL

class LoadFromID(LoadWorkflowMySQL):
    sql = LoadWorkflowMySQL.sql 