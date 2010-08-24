#!/usr/bin/env python
"""
_Parentage_

SQLite implementation of Fileset.New

"""
__all__ = []
__revision__ = "$Id: NewSQL.py,v 1.1 2008/06/09 16:30:08 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Fileset.NewSQL import New as NewFilesetMySQL
from WMCore.WMBS.SQLite.Base import SQLiteBase

class New(NewFilesetMySQL, SQLiteBase):
    sql = """insert into wmbs_fileset 
            (name, last_update) values (:fileset, :timestamp)"""
            
    def getBinds(self, fileset = None):
        binds = self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset',
            self.dbi.buildbinds(
                self.dbi.makelist(self.timestamp()), 'timestamp'))
        return binds