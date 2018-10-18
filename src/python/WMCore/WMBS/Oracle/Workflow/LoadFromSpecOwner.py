#!/usr/bin/env python
"""
_LoadFromSpecOwner_

Oracle implementation of Workflow.LoadFromSpecOwner

"""
__all__ = []



from WMCore.WMBS.MySQL.Workflow.LoadFromSpecOwner import LoadFromSpecOwner \
     as LoadWorkflowMySQL

class LoadFromSpecOwner(LoadWorkflowMySQL):
    sql = LoadWorkflowMySQL.sql
