#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of JobGroup.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.2 2009/01/16 22:38:02 sfoulkes Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """SELECT id, subscription, uid, output, last_update
             FROM wmbs_jobgroup WHERE id = :groupid"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id, subscription, output and last_update attributes to integers
        because formatDict() turns everything into strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        formattedResult["subscription"] = int(formattedResult["subscription"])
        formattedResult["output"] = int(formattedResult["output"])
        formattedResult["last_update"] = int(formattedResult["last_update"])
        return formattedResult

    def execute(self, id, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"groupid": id}, conn = conn,
                                      transaction = transaction)
        return self.formatDict(result)
