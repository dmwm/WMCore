#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Workflow.Exists

"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.2 2008/06/23 09:35:01 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Exists(MySQLBase):
    sql = """select id from wmbs_workflow
            where spec = :spec and owner = :owner and name = :name"""
    
    def format(self, result):
        result = MySQLBase.format(self, result)
        self.logger.debug( result )
        return result[0][0]
    
    def getBinds(self, spec=None, owner=None, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(owner), 'owner',
                                   self.dbi.buildbinds(self.dbi.makelist(spec), 'spec',
                                   self.dbi.buildbinds(self.dbi.makelist(name), 'name')))
        
    def execute(self, spec=None, owner=None, conn = None, name = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(spec, owner, name), 
                         conn = conn, transaction = transaction)
        return self.format(result)