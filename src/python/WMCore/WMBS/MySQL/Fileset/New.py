#!/usr/bin/env python
"""
_Parentage_
 
MySQL implementation of Fileset.New

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.2 2008/11/20 21:52:33 sryu Exp $"
__version__ = "$Revision: 1.2 $"


from WMCore.Database.DBFormatter import DBFormatter

class New(DBFormatter):
    sql = "insert into wmbs_fileset (name) values (:fileset)"
    
    def getBinds(self, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'fileset')
    
    def execute(self, name = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(name), 
                         conn = conn, transaction = transaction)
        return True #Or raise