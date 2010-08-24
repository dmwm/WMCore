#!/usr/bin/env python
"""
_LoadFromID_

MySQL implementation of Fileset.LoadFromID
"""

__all__ = []
__revision__ = "$Id: LoadFromID.py,v 1.6 2009/01/16 22:38:02 sfoulkes Exp $"
__version__ = "$Revision: 1.6 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromID(DBFormatter):
    sql = """SELECT id, name, open, last_update FROM wmbs_fileset
             WHERE id = :fileset"""

    def formatDict(self, result):
        """
        _formatDict_

        Cast the id and last_update attributes to integers because the
        DBFormatter's formatDict() method changes all attributes to be
        strings.
        """
        formattedResult = DBFormatter.formatDict(self, result)[0]
        formattedResult["id"] = int(formattedResult["id"])
        formattedResult["last_update"] = int(formattedResult["last_update"])
        return formattedResult
            
    def execute(self, fileset = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"fileset": fileset}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)
