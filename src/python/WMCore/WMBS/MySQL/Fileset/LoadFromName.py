#!/usr/bin/env python
"""
_LoadFromName_

MySQL implementation of Fileset.LoadFromName
"""

__all__ = []
__revision__ = "$Id: LoadFromName.py,v 1.5 2009/01/13 16:43:09 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class LoadFromName(DBFormatter):
    sql = """SELECT id, name, open, last_update FROM wmbs_fileset
             WHERE name = :fileset"""
            
    def execute(self, fileset = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, {"fileset": fileset}, 
                         conn = conn, transaction = transaction)
        return self.formatDict(result)[0]
