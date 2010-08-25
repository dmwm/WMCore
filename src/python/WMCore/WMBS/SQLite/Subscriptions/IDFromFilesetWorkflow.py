#!/usr/bin/env python
"""
_IDFromFilesetWorkflow_

SQLite implementation of Subscription.IDFromFilesetWorkflow
"""

__revision__ = "$Id: IDFromFilesetWorkflow.py,v 1.1 2009/07/24 19:08:11 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.IDFromFilesetWorkflow import IDFromFilesetWorkflow \
     as IDFromFilesetWorkflowMySQL

class IDFromFilesetWorkflow(IDFromFilesetWorkflowMySQL):
    pass
