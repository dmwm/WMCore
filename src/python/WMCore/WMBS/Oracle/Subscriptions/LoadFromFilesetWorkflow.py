#!/usr/bin/env python
"""
_LoadFromFilesetWorkflow_

Oracle implementation of Subscription.LoadFromFilesetWorkflow
"""

__all__ = []
__revision__ = "$Id: LoadFromFilesetWorkflow.py,v 1.2 2009/01/16 22:29:23 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Subscriptions.LoadFromFilesetWorkflow \
     import LoadFromFilesetWorkflow as LoadFromFilesetWorkflowMySQL

class LoadFromFilesetWorkflow(LoadFromFilesetWorkflowMySQL):
    sql = """SELECT wmbs_subscription.id, fileset, workflow, split_algo, name,
             last_update FROM wmbs_subscription INNER JOIN wmbs_subs_type ON
             wmbs_subs_type.id = subtype WHERE fileset = :fileset AND
             workflow = :workflow"""

    def formatDict(self, result):
        """
        _formatDict_

        Rename the name column to type to maintain compatibility with the
        MySQL DAO objects.
        """
        formattedResult = LoadFromFilesetWorkflowMySQL.formatDict(self, result)
        formattedResult["type"] = formattedResult["name"]
        del formattedResult["name"]
        return formattedResult
