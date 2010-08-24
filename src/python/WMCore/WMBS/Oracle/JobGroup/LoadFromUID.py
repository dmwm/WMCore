#!/usr/bin/env python
"""
_LoadFromUID_

Oracle implementation of JobGroup.LoadFromUID
"""

__all__ = []



from WMCore.WMBS.MySQL.JobGroup.LoadFromUID import LoadFromUID as LoadFromUIDMySQL

class LoadFromUID(LoadFromUIDMySQL):
    sql = """SELECT id, subscription, guid, output, last_update
             FROM wmbs_jobgroup WHERE guid = :guid"""

    def formatDict(self, result):
        """
        _formatDict_

        Rename the guid column to uid to keep compatibility with the MySQL DAO
        objects.
        """
        formattedResult = LoadFromUIDMySQL.formatDict(self, result)
        formattedResult["uid"] = formattedResult["guid"]
        del formattedResult["guid"]
        return formattedResult
