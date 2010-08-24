#!/usr/bin/env python
"""
_LoadFromID_

Oracle implementation of JobGroup.LoadFromID
"""

__all__ = []



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
        formattedResult = LoadFromIDMySQL.formatDict(self, result)
        formattedResult["uid"] = formattedResult["guid"]
        del formattedResult["guid"]
        return formattedResult
