#!/usr/bin/env python
"""
_Parentage_

SQLite implementation of Fileset.New

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/11/20 21:54:26 sryu Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Fileset.New import New as NewFilesetMySQL

class New(NewFilesetMySQL):
    sql = """insert into wmbs_fileset 
            (name, last_update) values (:fileset, :timestamp)"""
            
    def getBinds(self, fileset = None):
        binds = self.dbi.buildbinds(self.dbi.makelist(fileset), 'fileset',
            self.dbi.buildbinds(
                self.dbi.makelist(self.timestamp()), 'timestamp'))
        return binds