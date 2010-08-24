#!/usr/bin/env python
"""
_LoadFromFilesetWorkflow_

SQLite implementation of Subscription.LoadFromFilesetWorkflow
"""

__all__ = []



from WMCore.WMBS.MySQL.Subscriptions.LoadFromFilesetWorkflow \
     import LoadFromFilesetWorkflow as LoadFromFilesetWorkflowMySQL

class LoadFromFilesetWorkflow(LoadFromFilesetWorkflowMySQL):
    sql = LoadFromFilesetWorkflowMySQL.sql
