#!/usr/bin/env python
"""
MySQL implementation of File.Heritage

Make the parentage link between two file id's
"""
__all__ = []
__revision__ = "$Id: Heritage.py,v 1.4 2008/11/24 21:47:01 sryu Exp $"
__version__ = "$Revision: 1.4 $"

from WMCore.Database.DBFormatter import DBFormatter

class Heritage(DBFormatter):
    sql = """insert into wmbs_file_parent (child, parent) values (:child, :parent)"""
    
    def getBinds(self, parent=0, child=0):
        # Can't use self.dbi.buildbinds here...
        binds = []
        
        parent = self.dbi.makelist(parent)
        child = self.dbi.makelist(child)
        for p in parent:
            for c in child:
                binds.append({'child': c, 
                              'parent': p})
        return binds
    
    def format(self, result):
        return True
    
    def execute(self, parent=0, child=0, conn = None, transaction = False):
        binds = self.getBinds(parent, child)
        
        self.logger.debug('File.Heritage binds: %s' % binds)
        result = self.dbi.processData(self.sql, binds, 
                         conn = conn, transaction = transaction)
        return self.format(result)