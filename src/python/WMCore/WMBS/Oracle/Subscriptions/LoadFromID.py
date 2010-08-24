#!/usr/bin/env python
"""
_LoadFromID_

Oracle implementation of Subscription.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2009/01/14 16:35:24 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Subscriptions.LoadFromID import LoadFromID as LoadFromIDMySQL

class LoadFromID(LoadFromIDMySQL):
    sql = """SELECT wmbs_subscription.id, fileset, workflow, split_algo, name,
             last_update FROM wmbs_subscription INNER JOIN wmbs_subs_type ON
             wmbs_subs_type.id = subtype WHERE wmbs_subscription.id = :id"""

    def formatDict(self, result):
        """
        _formatDict_

        Rename the name column to type to maintain compatibility with the
        MySQL DAO objects.
        """
        formattedResults = LoadFromIDMySQL.formatDict(self, result)

        output = []
        for formattedResult in formattedResults:
            formattedResult["type"] = formattedResult["name"]
            del formattedResult["name"]
            output.append(formattedResult)
            
        return output
