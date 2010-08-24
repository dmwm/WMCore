#!/usr/bin/env python
"""
_Parentage_
 
MySQL implementation of Fileset.New

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.3 2008/11/24 21:47:03 sryu Exp $"
__version__ = "$Revision: 1.3 $"


from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = "insert into wmbs_fileset (name, open) values (:fileset, :open)"
    
    def getBinds(self, name = None, open=0):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'fileset',
                        self.dbi.buildbinds(
                            self.dbi.makelist(open), 'open'))
    
    def format(self, result):
        return True
    
    def execute(self, name = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(name), 
                         conn = conn, transaction = transaction)
        return True #Or raise