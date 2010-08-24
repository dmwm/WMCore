#!/usr/bin/env python
"""
_LoadFromFilesetWorkflow_

SQLite implementation of Subscription.LoadFromFilesetWorkflow
"""

__all__ = []
__revision__ = "$Id: LoadFromFilesetWorkflow.py,v 1.1 2009/01/14 16:35:24 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.LoadFromFilesetWorkflow \
     import LoadFromFilesetWorkflow as LoadFromFilesetWorkflowMySQL

class LoadFromFilesetWorkflow(LoadFromFilesetWorkflowMySQL):
    sql = LoadFromFilesetWorkflowMySQL.sql
