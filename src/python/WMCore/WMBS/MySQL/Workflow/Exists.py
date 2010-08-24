#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Workflow.Exists

"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.3 2008/06/23 16:01:16 metson Exp $"
__version__ = "$Revision: 1.3 $"

from WMCore.WMBS.MySQL.Base import MySQLBase

class Exists(MySQLBase):
    sql = """select id from wmbs_workflow
            where spec = :spec and owner = :owner and name = :name"""
    
    def format(self, result):
        result = MySQLBase.format(self, result)
        try:
            return result[0][0]
        except Exception, e:
            self.logger.error('Exists Exception: %s' % e)
            self.logger.debug( 'Exists Result: %s' % result )
            return False
    
    def getBinds(self, spec=None, owner=None, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(owner), 'owner',
                                   self.dbi.buildbinds(self.dbi.makelist(spec), 'spec',
                                   self.dbi.buildbinds(self.dbi.makelist(name), 'name')))
        
    def execute(self, spec=None, owner=None, name = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(spec, owner, name), 
                         conn = conn, transaction = transaction)
        return self.format(result)