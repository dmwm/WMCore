#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of JobGroup.LoadFromID
"""

__all__ = []



from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """SELECT id, subscription, guid, output, last_update
             FROM wmbs_jobgroup WHERE id = :groupid"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, subscription, output and last_update attributes to integers
        because formatDict() turns everything into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"]           = int(formattedResult["id"])
        formattedResult["uid"]          = formattedResult["guid"]
        formattedResult["subscription"] = int(formattedResult["subscription"])
        formattedResult["output"]       = int(formattedResult["output"])
        formattedResult["last_update"]  = int(formattedResult["last_update"])
        del formattedResult['guid']
        return formattedResult

    def execute(self, id, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"groupid": id}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
