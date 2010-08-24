#!/usr/bin/env python
"""
_LoadFromID_

Oracle implementation of JobGroup.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.1 2009/01/14 16:35:25 sfoulkes Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.JobGroup.LoadFromID import LoadFromID as LoadFromIDMySQL

class LoadFromID(LoadFromIDMySQL):
    sql = """SELECT id, subscription, guid, output, last_update
             FROM wmbs_jobgroup WHERE id = :groupid"""
    
    def formatDict(self, result):
        """
        _formatDict_

        Rename the guid column to uid to keep compatibility with the MySQL DAO
        objects.
        """
        formattedResults = LoadFromIDMySQL.formatDict(self, result)

        output = []
        for formattedResult in formattedResults:
            formattedResult["uid"] = formattedResult["guid"]
            del formattedResult["guid"]
            output.append(formattedResult)
            
        return output
