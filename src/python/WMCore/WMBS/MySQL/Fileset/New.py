#!/usr/bin/env python
"""
_Parentage_
 
MySQL implementation of Fileset.New

"""
__all__ = []
__revision__ = "$Id: New.py,v 1.1 2008/06/12 10:02:00 metson Exp $"
__version__ = "$Revision: 1.1 $"


from WMCore.WMBS.MySQL.Base import MySQLBase

class New(MySQLBase):
    sql = "insert into wmbs_fileset (name) values (:fileset)"
    
    def getBinds(self, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(name), 'fileset')
    
    def execute(self, name = None, conn = None, transaction = False):
        self.dbi.processData(self.sql, self.getBinds(name), 
                         conn = conn, transaction = transaction)
        return True #Or raise