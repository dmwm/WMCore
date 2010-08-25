#!/usr/bin/env python
"""
_LoadFromFilesetWorkflow_

Oracle implementation of Subscription.LoadFromFilesetWorkflow
"""

__all__ = []
__revision__ = "$Id: LoadFromFilesetWorkflow.py,v 1.3 2009/10/12 21:11:12 sfoulkes Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Subscriptions.LoadFromFilesetWorkflow \
     import LoadFromFilesetWorkflow as LoadFromFilesetWorkflowMySQL

class LoadFromFilesetWorkflow(LoadFromFilesetWorkflowMySQL):
    pass
