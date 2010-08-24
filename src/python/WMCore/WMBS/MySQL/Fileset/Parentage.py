#!/usr/bin/env python
"""
_Parentage_

MySQL implementation of Fileset.Parentage

"""
__all__ = []
__revision__ = "$Id: Parentage.py,v 1.1 2008/06/12 10:02:02 metson Exp $"
__version__ = "$Revision: 1.1 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Parentage(MySQLBase):
    sql = """insert into wmbs_fileset_parent (child, parent) 
                values ((select id from wmbs_fileset where name = :child),
                (select id from wmbs_fileset where name = :parent))"""
                
    def getBinds(self, child = 0, parent = 0):
        return self.dbi.buildbinds(child, 'child', self.dbi.buildbinds(parent, 'parent'))
    
    def execute(self, child = 0, parent = 0, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(child, parent), 
                         conn = conn, transaction = transaction)
        return self.format(result)