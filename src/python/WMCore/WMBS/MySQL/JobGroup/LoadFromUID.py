#!/usr/bin/env python
"""
_LoadFromUID_

MySQL implementation of JobGroup.LoadFromUID
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class LoadFromUID(DBFormatter):
    sql = """SELECT id, subscription, guid, output, last_update
             FROM wmbs_jobgroup WHERE guid = :guid"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, subscription, output and last_update attributes to integers
        because formatDict() turns everything into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"]           = int(formattedResult["id"])
        formattedResult["subscription"] = int(formattedResult["subscription"])
        formattedResult["output"]       = int(formattedResult["output"])
        formattedResult["last_update"]  = int(formattedResult["last_update"])
        formattedResult["uid"]          = formattedResult["guid"]
        del formattedResult['guid']
        return formattedResult

    def execute(self, uid, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"guid": uid}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
