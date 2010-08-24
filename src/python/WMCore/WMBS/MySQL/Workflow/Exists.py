#!/usr/bin/env python
"""
_Exists_

MySQL implementation of Workflow.Exists

"""
__all__ = []
__revision__ = "$Id: Exists.py,v 1.5 2008/11/21 17:09:01 sfoulkes Exp $"
__version__ = "$Revision: 1.5 $"

from WMCore.Database.DBFormatter import DBFormatter

class Exists(DBFormatter):
    sql = """select id from wmbs_workflow
            where spec = :spec and owner = :owner and name = :name"""
    
    def format(self, result):
        result = DBFormatter.format(self, result)

        if len(result) == 0:
            return False
        else:
            return result[0][0]
    
    def getBinds(self, spec=None, owner=None, name = None):
        return self.dbi.buildbinds(self.dbi.makelist(owner), 'owner',
                                   self.dbi.buildbinds(self.dbi.makelist(spec), 'spec',
                                   self.dbi.buildbinds(self.dbi.makelist(name), 'name')))
        
    def execute(self, spec=None, owner=None, name = None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(spec, owner, name), 
                         conn = conn, transaction = transaction)
        return self.format(result)
