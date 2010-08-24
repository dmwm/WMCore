#!/usr/bin/env python
"""
_Load_

MySQL implementation of Workflow.Load

"""
__all__ = []
__revision__ = "$Id: LoadSpecOwner.py,v 1.2 2008/10/09 09:55:27 metson Exp $"
__version__ = "$Revision: 1.2 $"

from WMCore.WMBS.MySQL.Workflow.LoadFromID import LoadFromID
    
class LoadSpecOwner(LoadFromID):
    sql = """select id, spec, name, owner from wmbs_workflow where spec = :spec and owner=:owner"""
    def getBinds(self, spec = None, owner=None):
        return self.dbi.buildbinds(self.dbi.makelist(spec), 'spec', 
                                   self.dbi.buildbinds(self.dbi.makelist(owner), 'owner'))
    
    def execute(self, spec = None, owner=None, conn = None, transaction = False):
        result = self.dbi.processData(self.sql, self.getBinds(spec, owner), 
                         conn = conn, transaction = transaction)
        return self.format(result)    