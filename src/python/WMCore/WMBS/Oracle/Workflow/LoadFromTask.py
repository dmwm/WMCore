#!/usr/bin/env python
"""
_LoadFromTask_

Oracle implementation of Workflow.LoadFromTask
"""

__all__ = []



from WMCore.WMBS.MySQL.Workflow.LoadFromTask import LoadFromTask as LoadFromTaskMySQL

class LoadFromTask(LoadFromTaskMySQL):
    sql = LoadFromTaskMySQL.sql
